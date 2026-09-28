"""Entry point: `python -m circuit_breaker` (run from the repository root)."""

from __future__ import annotations

import asyncio
import logging
import sys

from followsm_sdk import FollowSMClient

from circuit_breaker.breaker import CircuitBreaker
from circuit_breaker.callbacks import WebhookNotifier, cancel_on_halt, log_event
from circuit_breaker.config import BreakerConfig, load_config
from circuit_breaker.streams import PRICING_URL, EnterpriseRequiredError, guard_stream

log = logging.getLogger("circuit_breaker")

ENTERPRISE_CTA = (
    "\n  The tick-level WebSocket streams are an ENTERPRISE feature ($499/mo: /ws/v1/toxicity, "
    "/ws/v1/confluence, 1,000 req/min REST).\n"
    f"  -> {PRICING_URL}\n"
    "  Then set FOLLOWSM_API_KEY in .env and restart.\n"
)


async def cancel_maker_quotes(symbol: str) -> None:
    # Replace with your OMS call, e.g. `await asyncio.to_thread(clob.cancel_orders, open_ids[symbol])`.
    log.warning("KILL SWITCH: cancelling every resting maker quote on %s", symbol)


async def report_latency(breaker: CircuitBreaker, every_secs: float = 30.0) -> None:
    while True:
        await asyncio.sleep(every_secs)
        log.info("Breaker evaluation latency: %s", breaker.latency.summary())


async def run(config: BreakerConfig) -> None:
    if not config.followsm_api_key:
        raise EnterpriseRequiredError("FOLLOWSM_API_KEY is not set")

    client = FollowSMClient(api_key=config.followsm_api_key, risk_config=config.risk)
    webhook = WebhookNotifier(config.webhook_url, config.webhook_secret) if config.webhook_url else None
    callbacks = [log_event, cancel_on_halt(cancel_maker_quotes)] + ([webhook] if webhook else [])
    breaker = CircuitBreaker(config, callbacks)
    log.info("Guarding %s", ", ".join(sorted(config.symbols)) or "every streamed symbol")

    tasks = [
        asyncio.create_task(guard_stream("toxicity", breaker, client.stream_toxicity, breaker.on_toxicity)),
        asyncio.create_task(guard_stream("confluence", breaker, client.stream_confluence, breaker.on_confluence)),
        asyncio.create_task(report_latency(breaker)),
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await breaker.drain()
        if webhook:
            await webhook.aclose()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        asyncio.run(run(load_config()))
    except EnterpriseRequiredError as exc:
        log.error("%s%s", exc, ENTERPRISE_CTA)
        sys.exit(1)
    except KeyboardInterrupt:
        log.info("Stopped.")


if __name__ == "__main__":
    main()
