"""Pre-trade microstructure risk gate on followsm-sdk >= 1.7.

    gate = RiskGate(session_fetcher(api_key="..."))
    decision = gate.check("BTCUSDT")
    if decision.allow:
        place_order(...)
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional

import requests
from followsm_sdk import ConfluenceSnapshot, RateLimitExceededException, RiskConfig, is_toxic_book

API = "https://follow-sm.com/api/v1"
Fetch = Callable[[str], ConfluenceSnapshot]


def session_fetcher(api_key: Optional[str] = None, base_url: str = API, timeout: float = 2.0) -> Fetch:
    """One snapshot per call over a kept-alive connection (no new TLS handshake per order)."""
    session = requests.Session()
    if api_key:
        session.headers["X-FollowSM-Key"] = api_key

    def fetch(symbol: str) -> ConfluenceSnapshot:
        r = session.get(f"{base_url}/developer/confluence/snapshot", params={"symbol": symbol}, timeout=timeout)
        if r.status_code == 429:
            raise RateLimitExceededException(r.text, int(r.headers.get("X-RateLimit-Reset", 0)))
        if r.status_code != 200:  # 202 = symbol not tracked yet, subscribing; 404 = no fresh data
            raise requests.HTTPError(f"{r.status_code}: {r.text[:120]}", response=r)
        return ConfluenceSnapshot.model_validate(r.json())

    return fetch


@dataclass
class GateConfig:
    vpin_pct_trip: float = 0.90
    vpin_pct_rearm: float = 0.80  # hysteresis: re-open only below this
    spike: float = 0.20  # max |1% bid share - its EWMA|
    ewma_alpha: float = 0.10
    cooldown_s: float = 5.0  # stay closed this long after the last trigger
    max_age_s: float = 3.0  # an older snapshot fails closed
    cache_s: float = 1.0  # orders within this window share one request
    book: RiskConfig = field(default_factory=RiskConfig)  # ob_imbalance_percentile tails


@dataclass
class Decision:
    allow: bool
    reasons: list[str]
    snapshot_age_s: Optional[float] = None


@dataclass
class _State:
    snapshot: Optional[ConfluenceSnapshot] = None
    fetched_at: float = 0.0
    triggers: list[str] = field(default_factory=list)
    ewma: Optional[float] = None
    closed: bool = False
    last_trigger_at: float = 0.0


class RiskGate:
    def __init__(self, fetch: Fetch, config: Optional[GateConfig] = None, clock: Callable[[], float] = time.time):
        self.fetch, self.config, self.clock = fetch, config or GateConfig(), clock
        self._states: dict[str, _State] = {}

    def check(self, symbol: str) -> Decision:
        c, now = self.config, self.clock()
        st = self._states.setdefault(symbol, _State())
        if st.snapshot is None or now - st.fetched_at >= c.cache_s:
            try:
                snapshot = self.fetch(symbol)
            except (RateLimitExceededException, requests.RequestException) as exc:
                return Decision(False, [f"no snapshot: {exc}"])  # fail closed
            st.snapshot, st.fetched_at = snapshot, now
            st.triggers = self._triggers(st, snapshot)
        age = now - st.snapshot.timestamp_ms / 1000
        if age > c.max_age_s:
            return Decision(False, [f"stale snapshot ({age:.1f} s old)"], age)

        if st.triggers:
            st.closed, st.last_trigger_at = True, now
            return Decision(False, st.triggers, age)
        if st.closed:
            pctl = st.snapshot.binance_microstructure.vpin_percentile
            if now - st.last_trigger_at < c.cooldown_s:
                return Decision(False, [f"cooling down ({now - st.last_trigger_at:.1f} s since the last trigger)"], age)
            if pctl is not None and pctl >= c.vpin_pct_rearm:
                return Decision(False, [f"vpin_percentile {pctl:.2f} still above re-arm {c.vpin_pct_rearm:.2f}"], age)
            st.closed = False
        return Decision(True, [], age)

    def _triggers(self, st: _State, snapshot: ConfluenceSnapshot) -> list[str]:
        c, m = self.config, snapshot.binance_microstructure
        out = []
        if m.sweep_active:
            side = "buy" if (m.last_sweep_side or 0) > 0 else "sell"
            out.append(f"{side} sweep ${m.last_sweep_notional_usd or 0:,.0f}")
        if m.vpin_percentile is not None and m.vpin_percentile >= c.vpin_pct_trip:
            out.append(f"vpin_percentile {m.vpin_percentile:.2f}")
        if is_toxic_book(m, c.book):
            out.append(f"lopsided 1% book (imbalance percentile {m.ob_imbalance_percentile})")
        band = m.depth_bands.get("1.0%")
        if band is not None:
            x = band.imbalance_ratio
            if st.ewma is not None and abs(x - st.ewma) > c.spike:
                out.append(f"1% depth spike: bid share {x:.2f} vs EWMA {st.ewma:.2f}")
            st.ewma = x if st.ewma is None else c.ewma_alpha * x + (1 - c.ewma_alpha) * st.ewma
        return out
