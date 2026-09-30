"""Per-symbol circuit-breaker state machine fed by /ws/v1/toxicity and /ws/v1/confluence frames.

Hot path is synchronous and allocation-light: every frame is evaluated in-memory
and callbacks are only *scheduled* (asyncio tasks), so a slow webhook can never
delay the evaluation of the next tick.
"""

from __future__ import annotations

import asyncio
import logging
import statistics
import time
from collections import deque
from dataclasses import dataclass
from typing import Awaitable, Callable, Deque, Dict, List, Literal, Optional, Sequence, Set, Tuple

from followsm_sdk import ConfluenceSnapshot, SymbolToxicityMetrics, evaluate_risk_action
from pydantic import BaseModel

from circuit_breaker.config import BreakerConfig

log = logging.getLogger(__name__)

RiskAction = Literal["NONE", "WIDEN_SPREAD_1_5X", "WIDEN_SPREAD_2X", "HALT_MAKER_QUOTES"]
StreamName = Literal["toxicity", "confluence"]
SEVERITY: Dict[str, int] = {"NONE": 0, "WIDEN_SPREAD_1_5X": 1, "WIDEN_SPREAD_2X": 2, "HALT_MAKER_QUOTES": 3}
HALT = "HALT_MAKER_QUOTES"
DEPTH_BAND_1PCT = "1.0%"


class RiskEvent(BaseModel):
    """Sent to every callback whenever a symbol's effective action changes."""

    symbol: str
    action: RiskAction
    previous_action: RiskAction
    source: Literal["toxicity", "confluence", "connection"]
    reasons: List[str]
    vpin: Optional[float] = None
    vpin_percentile: Optional[float] = None
    ob_toxicity_1pct: Optional[float] = None
    imbalance_1pct: Optional[float] = None
    feed_age_ms: Optional[float] = None
    eval_latency_us: float
    emitted_at: float


Callback = Callable[[RiskEvent], Awaitable[None]]


@dataclass
class _SymbolState:
    tox_action: str = "NONE"
    conf_action: str = "NONE"
    tripped_at: float = 0.0
    imbalance_ewma: Optional[float] = None
    last_tox_ts: float = 0.0

    @property
    def effective(self) -> str:
        return max(self.tox_action, self.conf_action, key=SEVERITY.__getitem__)


class LatencyStats:
    """Rolling in-process evaluation latency (microseconds) over the last N frames."""

    def __init__(self, maxlen: int = 10_000) -> None:
        self._samples: Deque[float] = deque(maxlen=maxlen)

    def record(self, micros: float) -> None:
        self._samples.append(micros)

    def summary(self) -> str:
        if not self._samples:
            return "no frames yet"
        ordered = sorted(self._samples)
        p99 = ordered[int(0.99 * (len(ordered) - 1))]
        return (
            f"n={len(ordered)} p50={statistics.median(ordered):.1f}us "
            f"p99={p99:.1f}us max={ordered[-1]:.1f}us"
        )


class CircuitBreaker:
    """Combines a tick-level toxicity trip with the local RiskConfig confluence ladder.

    Effective action per symbol = the more severe of:
      * toxicity  : HALT while vpin_percentile > vpin_percentile_trip (raw VPIN > vpin_trip while the
                    percentile is unavailable), 1% book toxicity > ob_toxicity_trip or a 1%
                    depth-imbalance spike; re-arms only below the re-arm threshold and no
                    trip condition for `cooldown_secs` (hysteresis prevents flapping).
      * confluence: `evaluate_risk_action(snapshot, config.risk)`, i.e. the SDK's risk
                    ladder re-derived locally with your thresholds instead of the backend's.
    """

    def __init__(
        self,
        config: BreakerConfig,
        callbacks: Sequence[Callback],
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self.callbacks = list(callbacks)
        self._clock = clock
        self._states: Dict[str, _SymbolState] = {s: _SymbolState() for s in config.symbols}
        self._pending: Set[asyncio.Task] = set()
        self.latency = LatencyStats()

    def action(self, symbol: str) -> str:
        state = self._states.get(symbol)
        return state.effective if state else "NONE"

    def _watched(self, symbol: str) -> bool:
        return not self.config.symbols or symbol in self.config.symbols

    def _vpin_level(self, m: SymbolToxicityMetrics) -> Tuple[str, float, float, float]:
        """(label, value, trip, rearm): the percentile when published, raw VPIN otherwise."""
        c = self.config
        if m.vpin_percentile is not None:
            return "vpin_percentile", m.vpin_percentile, c.vpin_percentile_trip, c.vpin_percentile_rearm
        return "vpin", m.vpin, c.vpin_trip, c.vpin_rearm

    def _toxicity_reasons(self, state: _SymbolState, m: SymbolToxicityMetrics) -> List[str]:
        c = self.config
        reasons: List[str] = []
        label, level, trip, _ = self._vpin_level(m)
        if level > trip:
            reasons.append(f"{label} {level:.3f} > {trip:.2f}")
        if m.ob_toxicity_1pct > c.ob_toxicity_trip:
            reasons.append(f"ob_toxicity_1pct {m.ob_toxicity_1pct:.2f} > {c.ob_toxicity_trip:.2f}")
        band = m.depth_bands.get(DEPTH_BAND_1PCT)
        if band is not None:
            x, ewma = band.imbalance_ratio, state.imbalance_ewma
            if ewma is not None and abs(x - ewma) > c.imbalance_spike:
                reasons.append(f"1% depth imbalance spike {x:.2f} vs ewma {ewma:.2f}")
            a = c.imbalance_ewma_alpha
            state.imbalance_ewma = x if ewma is None else a * x + (1 - a) * ewma
        return reasons

    def on_toxicity(self, m: SymbolToxicityMetrics) -> Optional[RiskEvent]:
        started = time.perf_counter()
        if not self._watched(m.symbol):
            return None
        state = self._states.setdefault(m.symbol, _SymbolState())
        if m.timestamp <= state.last_tox_ts:
            return None  # already seen: a symbol can repeat on the stream without being recomputed
        state.last_tox_ts = m.timestamp
        previous = state.effective

        reasons = self._toxicity_reasons(state, m)
        label, level, _, rearm = self._vpin_level(m)
        now = self._clock()
        if reasons:
            state.tox_action, state.tripped_at = HALT, now
        elif (
            state.tox_action == HALT
            and level < rearm
            and now - state.tripped_at >= self.config.cooldown_secs
        ):
            state.tox_action = "NONE"
            reasons = [f"re-armed: {label} {level:.3f} < {rearm:.2f}, clean for {self.config.cooldown_secs:g}s"]

        band = m.depth_bands.get(DEPTH_BAND_1PCT)
        return self._commit(
            m.symbol,
            state,
            previous,
            "toxicity",
            reasons,
            started,
            vpin=m.vpin,
            vpin_percentile=m.vpin_percentile,
            ob_toxicity_1pct=m.ob_toxicity_1pct,
            imbalance_1pct=band.imbalance_ratio if band else None,
            feed_age_ms=(time.time() - m.timestamp) * 1000,
        )

    def on_confluence(self, snapshot: ConfluenceSnapshot) -> Optional[RiskEvent]:
        started = time.perf_counter()
        if not self._watched(snapshot.symbol):
            return None
        state = self._states.setdefault(snapshot.symbol, _SymbolState())
        previous = state.effective

        state.conf_action = evaluate_risk_action(snapshot, self.config.risk)
        micro = snapshot.binance_microstructure
        band = micro.depth_bands.get(DEPTH_BAND_1PCT)
        return self._commit(
            snapshot.symbol,
            state,
            previous,
            "confluence",
            [f"local ladder {state.conf_action} (backend: {snapshot.composite_signals.recommended_action})"],
            started,
            vpin=micro.vpin,
            vpin_percentile=micro.vpin_percentile,
            ob_toxicity_1pct=micro.ob_toxicity_1pct,
            imbalance_1pct=band.imbalance_ratio if band else None,
            feed_age_ms=time.time() * 1000 - snapshot.timestamp_ms,
        )

    def halt_all(self, stream: StreamName, reason: str) -> List[RiskEvent]:
        """Fail closed: a dropped stream halts every symbol it was guarding until fresh frames re-arm it."""
        started = time.perf_counter()
        events = []
        for symbol, state in self._states.items():
            previous = state.effective
            if stream == "toxicity":
                state.tox_action, state.tripped_at = HALT, self._clock()
            else:
                state.conf_action = HALT
            event = self._commit(symbol, state, previous, "connection", [reason], started)
            if event:
                events.append(event)
        return events

    def _commit(
        self,
        symbol: str,
        state: _SymbolState,
        previous: str,
        source: str,
        reasons: List[str],
        started: float,
        **metrics: Optional[float],
    ) -> Optional[RiskEvent]:
        action = state.effective
        elapsed_us = (time.perf_counter() - started) * 1e6
        self.latency.record(elapsed_us)
        if action == previous:
            return None
        event = RiskEvent(
            symbol=symbol,
            action=action,
            previous_action=previous,
            source=source,
            reasons=reasons,
            eval_latency_us=elapsed_us,
            emitted_at=time.time(),
            **metrics,
        )
        for callback in self.callbacks:
            task = asyncio.create_task(self._run_callback(callback, event))
            self._pending.add(task)
            task.add_done_callback(self._pending.discard)
        return event

    async def _run_callback(self, callback: Callback, event: RiskEvent) -> None:
        try:
            await asyncio.wait_for(callback(event), self.config.callback_timeout_secs)
        except Exception:
            log.exception("Callback %r failed for %s %s", callback, event.symbol, event.action)

    async def drain(self) -> None:
        """Wait for every scheduled callback (tests and graceful shutdown)."""
        if self._pending:
            await asyncio.gather(*list(self._pending), return_exceptions=True)
