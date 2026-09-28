"""Offline tests: no network, no keys required. Run with `pytest -q`."""

import asyncio
import hashlib
import hmac
import json
from types import SimpleNamespace

import httpx
import pytest
from followsm_sdk import ConfluenceSnapshot, RiskConfig, SymbolToxicityMetrics
from websockets.exceptions import InvalidStatus

from circuit_breaker.breaker import CircuitBreaker
from circuit_breaker.callbacks import WebhookNotifier, cancel_on_halt
from circuit_breaker.config import BreakerConfig
from circuit_breaker.streams import EnterpriseRequiredError, guard_stream

_ts = iter(range(1, 10_000))


def tick(vpin=0.30, ob_tox=1.0, imbalance=0.50, symbol="BTCUSDT", vpin_percentile=None):
    return SymbolToxicityMetrics(
        symbol=symbol,
        timestamp=float(next(_ts)),
        price=68000.0,
        vpin=vpin,
        vpin_percentile=vpin_percentile,
        ob_toxicity_1pct=ob_tox,
        ob_imbalance_l1=0.5,
        depth_bands={"1.0%": {"bid_notional": 1.0, "ask_notional": 1.0, "imbalance_ratio": imbalance}},
        volume_z_score=0.0,
        natr_15m=0.1,
        taker_buy_ratio=0.5,
        is_toxic_alert=vpin > 0.7,
    )


def confluence(vpin, divergence, confidence=0.91):
    return ConfluenceSnapshot.model_validate(
        {
            "symbol": "BTCUSDT",
            "timestamp_ms": 1790212800000,
            "binance_microstructure": {
                "price": 68420.5, "vpin": vpin, "ob_toxicity_1pct": 1.0, "ob_imbalance_l1": 0.5,
                "depth_bands": {}, "volume_z_score": 0.0, "natr_15m": 0.1, "taker_buy_ratio": 0.5,
                "price_delta_15m_pct": -0.004,
            },
            "polymarket_confluence": {
                "active_events": [
                    {
                        "market_slug": "m", "question": "q", "condition_id": "c", "yes_token_id": "y",
                        "direction": "bullish_if_yes", "direction_confidence": confidence,
                        "implied_probability": 0.8, "prob_delta_15m": 0.09,
                        "clob_order_flow_imbalance": 0.7, "smart_money_whale_sweeps_1h_usdt": 0.0,
                    }
                ],
                "macro_event_risk_score": 0.5,
            },
            "composite_signals": {
                "is_toxic_alert": vpin > 0.7, "cross_market_divergence_flag": divergence,
                "recommended_action": "NONE",
            },
        }
    )


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def make_breaker(**overrides):
    clock, received = Clock(), []

    async def record(event):
        received.append(event)

    config = BreakerConfig(symbols={"BTCUSDT"}, cooldown_secs=5.0, **overrides)
    return CircuitBreaker(config, [record], clock=clock), clock, received


def test_vpin_trip_then_hysteresis_rearm():
    async def scenario():
        breaker, clock, received = make_breaker()
        breaker.on_toxicity(tick(vpin=0.90))
        assert breaker.action("BTCUSDT") == "HALT_MAKER_QUOTES"

        clock.now = 10.0
        breaker.on_toxicity(tick(vpin=0.80))  # inside the hysteresis band: stay halted
        assert breaker.action("BTCUSDT") == "HALT_MAKER_QUOTES"

        breaker.on_toxicity(tick(vpin=0.70))
        assert breaker.action("BTCUSDT") == "NONE"
        await breaker.drain()
        return [e.action for e in received]

    assert asyncio.run(scenario()) == ["HALT_MAKER_QUOTES", "NONE"]


def test_rearm_waits_for_cooldown():
    async def scenario():
        breaker, clock, _ = make_breaker()
        breaker.on_toxicity(tick(vpin=0.90))
        clock.now = 2.0
        breaker.on_toxicity(tick(vpin=0.40))
        assert breaker.action("BTCUSDT") == "HALT_MAKER_QUOTES"
        clock.now = 5.0
        breaker.on_toxicity(tick(vpin=0.40))
        assert breaker.action("BTCUSDT") == "NONE"

    asyncio.run(scenario())


def test_vpin_percentile_overrides_raw_vpin_when_published():
    async def scenario():
        breaker, clock, received = make_breaker()
        breaker.on_toxicity(tick(vpin=0.95, vpin_percentile=0.40))  # high raw VPIN, normal for this pair
        assert breaker.action("BTCUSDT") == "NONE"
        breaker.on_toxicity(tick(vpin=0.20, vpin_percentile=0.93))
        assert breaker.action("BTCUSDT") == "HALT_MAKER_QUOTES"
        clock.now = 10.0
        breaker.on_toxicity(tick(vpin=0.20, vpin_percentile=0.85))  # inside the 0.80-0.90 band
        assert breaker.action("BTCUSDT") == "HALT_MAKER_QUOTES"
        breaker.on_toxicity(tick(vpin=0.20, vpin_percentile=0.70))
        assert breaker.action("BTCUSDT") == "NONE"
        await breaker.drain()
        return received[0]

    event = asyncio.run(scenario())
    assert event.reasons == ["vpin_percentile 0.930 > 0.90"] and event.vpin_percentile == 0.93


def test_depth_imbalance_spike_trips():
    async def scenario():
        breaker, _, received = make_breaker()
        breaker.on_toxicity(tick(imbalance=0.50))
        breaker.on_toxicity(tick(imbalance=0.20))
        await breaker.drain()
        return received

    (event,) = asyncio.run(scenario())
    assert event.action == "HALT_MAKER_QUOTES"
    assert "imbalance spike" in event.reasons[0]


def test_duplicate_frames_and_unwatched_symbols_are_ignored():
    async def scenario():
        breaker, _, _ = make_breaker()
        frame = tick(vpin=0.90)
        assert breaker.on_toxicity(frame) is not None
        assert breaker.on_toxicity(frame) is None
        assert breaker.on_toxicity(tick(vpin=0.9, symbol="DOGEUSDT")) is None

    asyncio.run(scenario())


def test_confluence_uses_local_risk_config_fallback():
    async def scenario():
        breaker, _, _ = make_breaker(risk=RiskConfig(vpin_halt_threshold=0.75))
        breaker.on_confluence(confluence(vpin=0.78, divergence=True))
        assert breaker.action("BTCUSDT") == "HALT_MAKER_QUOTES"  # backend said NONE
        breaker.on_confluence(confluence(vpin=0.78, divergence=True, confidence=0.40))
        assert breaker.action("BTCUSDT") == "WIDEN_SPREAD_1_5X"  # low semantic confidence downgrade

    asyncio.run(scenario())


def test_cancel_on_halt_invokes_oms_once_per_transition():
    cancelled = []

    async def cancel_quotes(symbol):
        cancelled.append(symbol)

    async def scenario():
        config = BreakerConfig(symbols={"BTCUSDT"})
        breaker = CircuitBreaker(config, [cancel_on_halt(cancel_quotes)])
        breaker.on_toxicity(tick(vpin=0.90))
        breaker.on_toxicity(tick(vpin=0.92))
        await breaker.drain()

    asyncio.run(scenario())
    assert cancelled == ["BTCUSDT"]


def test_webhook_body_is_hmac_signed():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"], seen["sig"] = request.content, request.headers["X-FollowSM-Signature"]
        return httpx.Response(204)

    async def scenario():
        notifier = WebhookNotifier("https://oms.example/kill", secret="s3cret")
        notifier._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        breaker = CircuitBreaker(BreakerConfig(symbols={"BTCUSDT"}), [notifier])
        breaker.on_toxicity(tick(vpin=0.90))
        await breaker.drain()
        await notifier.aclose()

    asyncio.run(scenario())
    assert json.loads(seen["body"])["action"] == "HALT_MAKER_QUOTES"
    assert seen["sig"] == hmac.new(b"s3cret", seen["body"], hashlib.sha256).hexdigest()


def test_disconnect_fails_closed_and_403_raises_enterprise_required():
    attempts = []

    async def stream():
        attempts.append(1)
        if len(attempts) == 1:
            yield tick(vpin=0.30)
            return
        raise InvalidStatus(SimpleNamespace(status_code=403))

    async def scenario():
        breaker, _, _ = make_breaker()
        with pytest.raises(EnterpriseRequiredError):
            await guard_stream("toxicity", breaker, stream, breaker.on_toxicity, max_backoff_secs=0.01)
        return breaker.action("BTCUSDT")

    assert asyncio.run(scenario()) == "HALT_MAKER_QUOTES"
