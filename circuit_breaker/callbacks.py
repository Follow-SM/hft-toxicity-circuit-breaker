"""Ready-made RiskEvent callbacks: structured logging, maker-quote cancellation, signed webhooks."""

from __future__ import annotations

import hashlib
import hmac
import logging
from typing import Awaitable, Callable, Optional

import httpx

from circuit_breaker.breaker import HALT, Callback, RiskEvent

log = logging.getLogger(__name__)


async def log_event(event: RiskEvent) -> None:
    level = logging.WARNING if event.action == HALT else logging.INFO
    log.log(
        level,
        "%s %s -> %s [%s] %s | vpin=%s pctl=%s ob_tox_1pct=%s eval=%.1fus feed_age=%sms",
        event.symbol,
        event.previous_action,
        event.action,
        event.source,
        "; ".join(event.reasons),
        f"{event.vpin:.3f}" if event.vpin is not None else "-",
        f"{event.vpin_percentile:.2f}" if event.vpin_percentile is not None else "-",
        f"{event.ob_toxicity_1pct:.2f}" if event.ob_toxicity_1pct is not None else "-",
        event.eval_latency_us,
        f"{event.feed_age_ms:.0f}" if event.feed_age_ms is not None else "-",
    )


def cancel_on_halt(cancel_quotes: Callable[[str], Awaitable[None]]) -> Callback:
    """Adapt any `async cancel_quotes(symbol)` (your OMS / exchange client) into a breaker callback."""

    async def callback(event: RiskEvent) -> None:
        if event.action == HALT:
            await cancel_quotes(event.symbol)

    return callback


class WebhookNotifier:
    """POSTs every RiskEvent as JSON; signs the raw body with HMAC-SHA256 when a secret is set."""

    def __init__(self, url: str, secret: Optional[str] = None, timeout_secs: float = 2.0) -> None:
        self.url = url
        self._secret = secret.encode() if secret else None
        self._client = httpx.AsyncClient(timeout=timeout_secs)

    def sign(self, body: bytes) -> str:
        return hmac.new(self._secret, body, hashlib.sha256).hexdigest()

    async def __call__(self, event: RiskEvent) -> None:
        body = event.model_dump_json().encode()
        headers = {"Content-Type": "application/json"}
        if self._secret:
            headers["X-FollowSM-Signature"] = self.sign(body)
        response = await self._client.post(self.url, content=body, headers=headers)
        response.raise_for_status()

    async def aclose(self) -> None:
        await self._client.aclose()
