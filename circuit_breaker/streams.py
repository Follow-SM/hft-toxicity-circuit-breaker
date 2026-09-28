"""Self-healing `async for` consumers for client.stream_toxicity() / client.stream_confluence()."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator, Callable

from pydantic import ValidationError
from websockets.exceptions import ConnectionClosed, InvalidStatus, WebSocketException

from circuit_breaker.breaker import CircuitBreaker, StreamName

log = logging.getLogger(__name__)

PRICING_URL = "https://follow-sm.com/pricing"


class EnterpriseRequiredError(RuntimeError):
    """The WebSocket handshake was rejected: the key is missing, invalid or not on ENTERPRISE."""


def is_enterprise_rejection(exc: BaseException) -> bool:
    if isinstance(exc, InvalidStatus):
        return exc.response.status_code in (401, 403)
    if isinstance(exc, ConnectionClosed):
        return exc.rcvd is not None and exc.rcvd.code == 4003
    return False


async def guard_stream(
    name: StreamName,
    breaker: CircuitBreaker,
    open_stream: Callable[[], AsyncIterator[Any]],
    on_frame: Callable[[Any], object],
    max_backoff_secs: float = 30.0,
) -> None:
    """Consume one stream forever; halt its symbols on every disconnect, then reconnect with backoff."""
    backoff = 1.0
    while True:
        try:
            async for frame in open_stream():
                backoff = 1.0
                on_frame(frame)
            reason = f"{name} stream closed by server"
        except (WebSocketException, OSError, asyncio.TimeoutError, ValidationError) as exc:
            if is_enterprise_rejection(exc):
                raise EnterpriseRequiredError(
                    f"/ws/v1/{name} requires an ENTERPRISE API key ({PRICING_URL})"
                ) from exc
            reason = f"{name} stream error: {exc}"
        breaker.halt_all(name, reason)
        log.warning("%s; reconnecting in %.0fs", reason, backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, max_backoff_secs)
