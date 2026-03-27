from __future__ import annotations

import asyncio
import contextlib
import logging
from decimal import Decimal
from typing import Any

import httpx

from polymarket_arb.arbitrage_engine import ArbitrageScanner, YesNoMarket, fetch_yes_no_markets
from polymarket_arb.clients.clob import measure_clob_health_latency
from polymarket_arb.config import Settings
from polymarket_arb.market_streamer import MarketStreamer
from polymarket_arb.orderbook import OrderBookState
from polymarket_arb.shadow_executor import ShadowExecutor

logger = logging.getLogger(__name__)

RADAR_QUERY_TERMS = ("bitcoin", "btc")
RADAR_TITLE_TERMS = ("daily", "hourly", "price")
RADAR_REFRESH_SECONDS = 5 * 60
RADAR_MAX_MARKETS = 500
FALLBACK_BOOT_SLUGS = ("btc-updown-5m-1774571400",)


def _print_bootup_clob_latency_banner(latency_ms: float) -> None:
    """Highly visible stdout banner for colocated host latency verification."""
    msg = f"🚀 BOOTUP: Polymarket CLOB Network Latency: {latency_ms:.2f} ms"
    width = 80
    rule = "=" * width
    pad = width - 4
    print(f"\n{rule}", flush=True)
    print(f"||{' ' * pad}||", flush=True)
    print(f"||  {msg}{' ' * max(0, pad - 2 - len(msg))}||", flush=True)
    print(f"||{' ' * pad}||", flush=True)
    print(f"{rule}\n", flush=True)


async def run_health_probe(settings: Settings | None = None) -> None:
    """Optional probe: GET CLOB ``/`` (same as ``py-clob-client`` ``get_ok``)."""

    from polymarket_arb.clients.clob import measure_clob_health_latency

    settings = settings or Settings.from_env()
    timeout = httpx.Timeout(settings.request_timeout_seconds)
    async with httpx.AsyncClient(timeout=timeout) as client:
        result = await measure_clob_health_latency(client, settings)
    if result.http_status >= 400:
        logger.error(
            "CLOB health check failed: status=%s latency_ms=%.2f body=%r",
            result.http_status,
            result.latency_ms,
            result.body_text[:200],
        )
        return
    logger.info(
        "CLOB health OK from your location: latency_ms=%.2f status=%s body=%r",
        result.latency_ms,
        result.http_status,
        result.body_text.strip(),
    )


def _dedupe_yes_no_markets(markets: list[YesNoMarket]) -> list[YesNoMarket]:
    seen: set[str] = set()
    out: list[YesNoMarket] = []
    for m in markets:
        key = (m.condition_id or "").strip() or f"{m.yes_token_id}|{m.no_token_id}"
        if key in seen:
            continue
        seen.add(key)
        out.append(m)
    return out


def _market_matches_radar(market: dict[str, Any]) -> bool:
    if market.get("active") is False:
        return False
    if market.get("closed") is True or market.get("archived") is True:
        return False
    query_text = " ".join(
        str(market.get(field) or "")
        for field in ("question", "title", "description", "groupItemTitle")
    ).lower()
    if not any(term in query_text for term in RADAR_QUERY_TERMS):
        return False
    title_text = " ".join(
        str(market.get(field) or "")
        for field in ("title", "question", "groupItemTitle")
    ).lower()
    return any(term in title_text for term in RADAR_TITLE_TERMS)


async def fetch_dynamic_market_slugs(
    http: httpx.AsyncClient,
    settings: Settings,
) -> list[str]:
    """
    Radar discovery: active/open Bitcoin/BTC markets from title patterns.

    Returns deduped event slugs first (preferred for Yes/No lookup), falling back to market slugs.
    """

    headers = {"User-Agent": settings.http_user_agent}
    url = f"{settings.gamma_api_base_url}/markets"
    params = {
        "active": "true",
        "closed": "false",
        "limit": str(RADAR_MAX_MARKETS),
        "offset": "0",
    }
    resp = await http.get(url, params=params, headers=headers)
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, list):
        return []
    event_slugs: list[str] = []
    market_slugs: list[str] = []
    seen_event: set[str] = set()
    seen_market: set[str] = set()
    for item in payload:
        if not isinstance(item, dict):
            continue
        if not _market_matches_radar(item):
            continue
        event_slug = str(item.get("eventSlug") or "").strip()
        market_slug = str(item.get("slug") or "").strip()
        if event_slug and event_slug not in seen_event:
            seen_event.add(event_slug)
            event_slugs.append(event_slug)
        if market_slug and market_slug not in seen_market:
            seen_market.add(market_slug)
            market_slugs.append(market_slug)
    return event_slugs + [slug for slug in market_slugs if slug not in seen_event]


async def resolve_yes_no_markets_for_slugs(
    http: httpx.AsyncClient,
    settings: Settings,
    slugs: list[str],
) -> list[YesNoMarket]:
    all_markets: list[YesNoMarket] = []
    for slug in slugs:
        try:
            chunk = await fetch_yes_no_markets(http, settings, slug)
            all_markets.extend(chunk)
        except ValueError as exc:
            logger.debug("Skipping slug %r: %s", slug, exc)
    return _dedupe_yes_no_markets(all_markets)


async def dynamic_market_radar_loop(
    *,
    settings: Settings,
    scanner: ArbitrageScanner,
    streamer: MarketStreamer,
) -> None:
    """Every 5 minutes discover Bitcoin/BTC title-matching markets and hot-swap."""

    timeout = httpx.Timeout(settings.request_timeout_seconds)
    headers = {"User-Agent": settings.http_user_agent}
    known_slugs: tuple[str, ...] = ()
    async with httpx.AsyncClient(timeout=timeout, headers=headers) as http:
        while True:
            try:
                slugs = await fetch_dynamic_market_slugs(http, settings)
                if not slugs:
                    logger.debug("Radar sweep returned no candidate slugs")
                else:
                    all_markets = await resolve_yes_no_markets_for_slugs(http, settings, slugs)
                    if all_markets:
                        slug_tuple = tuple(slugs)
                        if slug_tuple != known_slugs:
                            await scanner.update_markets(all_markets)
                            await streamer.update_subscriptions(slugs)
                            known_slugs = slug_tuple
                            logger.debug(
                                "Radar retargeted to %d slug(s), %d Yes/No market(s)",
                                len(slugs),
                                len(all_markets),
                            )
                    else:
                        logger.debug("Radar resolved slugs but found no Yes/No pairs")
            except asyncio.CancelledError:
                break
            except Exception:
                logger.debug("dynamic market radar loop error", exc_info=True)
            try:
                await asyncio.sleep(RADAR_REFRESH_SECONDS)
            except asyncio.CancelledError:
                break


async def run() -> None:
    """
    Run market WebSocket, fee refresher, latency calibration, and arbitrage scanner concurrently.

    Dynamic radar: one WebSocket subscribes to discovered CLOB tokens while the scanner tracks
    Yes/No pairs for active Bitcoin/BTC price-action markets. Console stays quiet except
    ShadowExecutor banners.
    """

    settings = Settings.from_env()
    timeout = httpx.Timeout(settings.request_timeout_seconds)
    headers = {"User-Agent": settings.http_user_agent}
    slugs = ["btc-updown-5m-1774571400"]
    async with httpx.AsyncClient(timeout=timeout, headers=headers) as http:
        all_markets = await resolve_yes_no_markets_for_slugs(http, settings, slugs)
        boot = await measure_clob_health_latency(http, settings)
        _print_bootup_clob_latency_banner(boot.latency_ms)

    order_book = OrderBookState()
    shadow = ShadowExecutor(settings=settings, order_book=order_book)
    streamer = MarketStreamer(
        settings=settings,
        stream_slugs=slugs,
        order_book=order_book,
    )
    scanner = ArbitrageScanner(
        settings=settings,
        order_book=order_book,
        markets=all_markets,
        shadow_executor=shadow,
        target_profit_margin=Decimal("0.0015"),
    )
    await scanner.initialize()
    fee_task = asyncio.create_task(scanner.run_fee_refresh_loop(), name="fee_refresh")
    # Manual forced-test mode: keep radar disabled so targets are not overwritten.
    # radar_task = asyncio.create_task(
    #     dynamic_market_radar_loop(settings=settings, scanner=scanner, streamer=streamer),
    #     name="dynamic_market_radar",
    # )
    latency_task = asyncio.create_task(
        shadow.run_latency_calibration_loop(),
        name="shadow_latency_calibration",
    )
    try:
        await asyncio.gather(
            streamer.run_forever(),
            scanner.scan_for_opportunities(),
            fee_task,
            latency_task,
        )
    finally:
        streamer.stop()
        scanner.stop()
        shadow.stop()
        fee_task.cancel()
        latency_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await fee_task
        with contextlib.suppress(asyncio.CancelledError):
            await latency_task
