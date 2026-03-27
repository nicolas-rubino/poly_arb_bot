from __future__ import annotations

import time
from dataclasses import dataclass

import httpx

from polymarket_arb.config import Settings


@dataclass(frozen=True, slots=True)
class ClobHealthResult:
    """Outcome of a CLOB health probe (matches official py-clob-client `get_ok`)."""

    latency_ms: float
    http_status: int
    body_text: str


async def measure_clob_health_latency(
    client: httpx.AsyncClient,
    settings: Settings,
) -> ClobHealthResult:
    """
    GET {clob_base_url}/ — public health check; no authentication.

    See Polymarket py-clob-client `ClobClient.get_ok()` which uses the same URL.
    """
    url = f"{settings.clob_base_url}/"
    started = time.perf_counter()
    response = await client.get(url)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    body_text = response.text
    return ClobHealthResult(
        latency_ms=elapsed_ms,
        http_status=response.status_code,
        body_text=body_text,
    )
