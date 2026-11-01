"""Offline tests for risk_gate.py (no network)."""

import pytest
import requests
from followsm_sdk import ConfluenceSnapshot, RateLimitExceededException

from risk_gate import GateConfig, RiskGate

NOW = 1_800_000_000.0


def snap(age=0.2, sweep=False, vpin_pct=0.5, obp=0.5, bid_share=0.5, now=NOW):
    return ConfluenceSnapshot.model_validate(
        {
            "symbol": "BTCUSDT",
            "timestamp_ms": int((now - age) * 1000),
            "binance_microstructure": {
                "price": 70_000.0, "vpin": 0.3, "vpin_percentile": vpin_pct, "ob_toxicity_1pct": 1.0,
                "ob_imbalance_l1": 0.5,
                "depth_bands": {"1.0%": {"bid_notional": bid_share, "ask_notional": 1 - bid_share, "imbalance_ratio": bid_share}},
                "volume_z_score": 0.0, "natr_15m": 0.002, "taker_buy_ratio": 0.5, "price_delta_15m_pct": 0.0,
                "ob_imbalance_percentile": obp,
                "sweep_active": sweep, "last_sweep_side": -1 if sweep else 0, "last_sweep_notional_usd": 184_250.0 if sweep else 0.0,
            },
            "polymarket_confluence": {"active_events": [], "macro_event_risk_score": 0.0},
            "composite_signals": {"is_toxic_alert": False, "cross_market_divergence_flag": False, "recommended_action": "NONE"},
        }
    )


class Feed:
    """Serves snapshots stamped against the test clock; `next` holds snap() kwargs or an exception."""

    def __init__(self, clock):
        self.calls, self.next, self.clock = 0, {}, clock

    def __call__(self, symbol):
        self.calls += 1
        if isinstance(self.next, Exception):
            raise self.next
        return snap(now=self.clock.now, **self.next)


class Clock:
    def __init__(self):
        self.now = NOW

    def __call__(self):
        return self.now


def gate():
    clock = Clock()
    feed = Feed(clock)
    return RiskGate(feed, GateConfig(), clock), feed, clock


def test_calm_book_allows_and_caches_within_a_second():
    g, feed, clock = gate()
    assert g.check("BTCUSDT").allow and g.check("BTCUSDT").allow
    assert feed.calls == 1
    clock.now += 1.0
    g.check("BTCUSDT")
    assert feed.calls == 2


def test_sweep_closes_then_cooldown_and_hysteresis_reopen():
    g, feed, clock = gate()
    feed.next = dict(sweep=True, vpin_pct=0.85)
    d = g.check("BTCUSDT")
    assert not d.allow and d.reasons == ["sell sweep $184,250"]
    feed.next = dict(vpin_pct=0.85)
    clock.now += 2
    assert "cooling down" in g.check("BTCUSDT").reasons[0]
    clock.now += 4
    assert "still above re-arm" in g.check("BTCUSDT").reasons[0]  # 0.85 is between rearm 0.80 and trip 0.90
    feed.next = dict(vpin_pct=0.70)
    clock.now += 1
    assert g.check("BTCUSDT").allow


def test_vpin_book_and_spike_triggers():
    g, feed, clock = gate()
    feed.next = dict(vpin_pct=0.95, obp=0.995)
    reasons = g.check("BTCUSDT").reasons
    assert reasons[0].startswith("vpin_percentile 0.95") and reasons[1].startswith("lopsided 1% book")
    g2, feed2, clock2 = gate()
    g2.check("BTCUSDT")
    feed2.next, clock2.now = dict(bid_share=0.8), clock2.now + 1
    assert g2.check("BTCUSDT").reasons == ["1% depth spike: bid share 0.80 vs EWMA 0.50"]


@pytest.mark.parametrize("failure", [RateLimitExceededException("429", 0), requests.ConnectionError("down")])
def test_fails_closed_without_a_snapshot(failure):
    g, feed, _ = gate()
    feed.next = failure
    d = g.check("BTCUSDT")
    assert not d.allow and d.reasons[0].startswith("no snapshot")


def test_stale_snapshot_fails_closed():
    g, feed, _ = gate()
    feed.next = dict(age=5.0)
    assert g.check("BTCUSDT").reasons == ["stale snapshot (5.0 s old)"]
