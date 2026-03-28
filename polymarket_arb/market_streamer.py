"""
Live Polymarket CLOB order book streaming (async).

Subscribes to the public market WebSocket (official CLOB channel), resolves an
*event* or *market* slug via the Gamma API to outcome token (asset) IDs, feeds
``price_change`` (and ``book``) top-of-book fields into :class:`~polymarket_arb.orderbook.OrderBookState`,
and can emit spread alerts when the bid/ask spread tightens below a threshold.

Endpoint reference: https://docs.polymarket.com/developers/CLOB/websocket/market-channel
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import httpx
import websockets
from websockets.exceptions import ConnectionClosed

from polymarket_arb.config import Settings
from polymarket_arb.orderbook import OrderBookState

logger = logging.getLogger(__name__)


def _parse_json_field(value: Any) -> Any:
    if isinstance(value, str) and value:
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _best_from_book_levels(
    bids: Sequence[Mapping[str, Any]],
    asks: Sequence[Mapping[str, Any]],
) -> tuple[Decimal, Decimal, Decimal, Decimal] | None:
    """Best bid = max price among bids; best ask = min price among asks."""
    best_bid: tuple[Decimal, Decimal] | None = None
    best_ask: tuple[Decimal, Decimal] | None = None
    for row in bids:
        if not isinstance(row, Mapping) or "price" not in row:
            continue
        bid_price = Decimal(str(row["price"]).strip())
        bid_size = Decimal(str(row.get("size", row.get("quantity", "0"))).strip())
        if best_bid is None or bid_price > best_bid[0]:
            best_bid = (bid_price, bid_size)
    for row in asks:
        if not isinstance(row, Mapping) or "price" not in row:
            continue
        ask_price = Decimal(str(row["price"]).strip())
        ask_size = Decimal(str(row.get("size", row.get("quantity", "0"))).strip())
        if best_ask is None or ask_price < best_ask[0]:
            best_ask = (ask_price, ask_size)
    if best_bid is None or best_ask is None:
        return None
    return best_bid[0], best_bid[1], best_ask[0], best_ask[1]


@dataclass(frozen=True, slots=True)
class OrderBookLevel:
    """Single price level (size optional when quoting from best bid/ask only)."""

    price: Decimal
    size: Decimal


@dataclass(frozen=True, slots=True)
class TopOfBook:
    """Best bid / ask for one outcome token."""

    best_bid: OrderBookLevel | None
    best_ask: OrderBookLevel | None


@dataclass(frozen=True, slots=True)
class TopOfBookSnapshot:
    """
    Immutable top-of-book update suitable for strategies or execution hooks.

    ``source_event`` mirrors Polymarket ``event_type`` (``book``, ``price_change``, …).
    """

    event_slug: str
    asset_id: str
    outcome_label: str
    source_event: str
    top: TopOfBook


@dataclass(frozen=True, slots=True)
class StreamTarget:
    """One tradeable outcome token on the CLOB."""

    asset_id: str
    outcome_label: str


class GammaSlugResolver:
    """
    Resolves a Polymarket *event* or *market* slug to CLOB asset IDs.

    Event slugs (parent groupings with many strikes) return every enabled
    ``clobTokenIds`` under that event so all outcome books stream together.
    Use :meth:`resolve_targets_for_slugs` to merge many slugs into one token list (deduped).
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def resolve_targets_for_slugs(
        self,
        client: httpx.AsyncClient,
        slugs: Sequence[str],
    ) -> list[StreamTarget]:
        """
        Resolve several Gamma slugs and return a single deduped list of :class:`StreamTarget`.

        Each outcome label is prefixed with ``[slug]`` so multi-market streams stay identifiable.
        Order is stable: first slug first, then first-seen asset IDs only.
        """

        if not slugs:
            raise ValueError("At least one slug is required for streaming")
        seen_ids: set[str] = set()
        merged: list[StreamTarget] = []
        for slug in slugs:
            s = str(slug).strip()
            if not s:
                continue
            try:
                batch = await self.resolve_targets(client, s)
            except (ValueError, httpx.HTTPError) as exc:
                logger.debug("Skipping stream slug %r: %s", s, exc)
                continue
            for t in batch:
                if t.asset_id in seen_ids:
                    continue
                seen_ids.add(t.asset_id)
                prefixed = StreamTarget(
                    asset_id=t.asset_id,
                    outcome_label=f"[{s}] {t.outcome_label}",
                )
                merged.append(prefixed)
        if not merged:
            raise ValueError(f"No CLOB targets resolved for slugs={list(slugs)!r}")
        return merged

    async def resolve_targets(self, client: httpx.AsyncClient, slug: str) -> list[StreamTarget]:
        headers = {"User-Agent": self._settings.http_user_agent}
        event_url = f"{self._settings.gamma_api_base_url}/events"
        resp = await client.get(event_url, params={"slug": slug}, headers=headers)
        resp.raise_for_status()
        events = resp.json()
        if isinstance(events, list) and events:
            return self._targets_from_event(events[0], slug)
        market_url = f"{self._settings.gamma_api_base_url}/markets"
        mresp = await client.get(market_url, params={"slug": slug}, headers=headers)
        mresp.raise_for_status()
        markets = mresp.json()
        if not isinstance(markets, list) or not markets:
            msg = f"No Gamma event or market found for slug={slug!r}"
            raise ValueError(msg)
        return self._targets_from_market(markets[0])

    def _targets_from_event(self, event: Mapping[str, Any], event_slug: str) -> list[StreamTarget]:
        markets = event.get("markets") or []
        if not isinstance(markets, list) or not markets:
            raise ValueError(f"Gamma event {event_slug!r} has no markets")
        targets: list[StreamTarget] = []
        for m in markets:
            if not m.get("enableOrderBook", False):
                continue
            raw_ids = m.get("clobTokenIds")
            ids: list[str]
            if isinstance(raw_ids, str):
                parsed = json.loads(raw_ids)
                ids = [str(x) for x in parsed]
            elif isinstance(raw_ids, list):
                ids = [str(x) for x in raw_ids]
            else:
                continue
            outcomes = _parse_json_field(m.get("outcomes")) or []
            if not isinstance(outcomes, list):
                outcomes = []
            title = str(m.get("groupItemTitle") or m.get("question") or "outcome")
            for i, asset_id in enumerate(ids):
                outcome = str(outcomes[i]) if i < len(outcomes) else f"#{i}"
                targets.append(StreamTarget(asset_id=asset_id, outcome_label=f"{title} · {outcome}"))
        if not targets:
            raise ValueError(f"No CLOB-enabled markets with token IDs for event slug={event_slug!r}")
        return targets

    def _targets_from_market(self, m: Mapping[str, Any]) -> list[StreamTarget]:
        raw_ids = m.get("clobTokenIds")
        if isinstance(raw_ids, str):
            ids = [str(x) for x in json.loads(raw_ids)]
        elif isinstance(raw_ids, list):
            ids = [str(x) for x in raw_ids]
        else:
            raise ValueError("Market response missing clobTokenIds")
        outcomes = _parse_json_field(m.get("outcomes")) or []
        if not isinstance(outcomes, list):
            outcomes = []
        title = str(m.get("groupItemTitle") or m.get("question") or "market")
        targets = []
        for i, asset_id in enumerate(ids):
            outcome = str(outcomes[i]) if i < len(outcomes) else f"#{i}"
            targets.append(StreamTarget(asset_id=asset_id, outcome_label=f"{title} · {outcome}"))
        return targets


def format_spread_alert_line(
    *,
    outcome_label: str,
    asset_id: str,
    bid: Decimal,
    ask: Decimal,
    spread: Decimal,
    threshold: Decimal,
    source_event: str,
) -> str:
    short_asset = asset_id[:6] + "…" + asset_id[-4:]
    return (
        f"ALERT spread={spread} < {threshold} | {source_event:14} | {outcome_label} | "
        f"bid={bid} ask={ask} | {short_asset}"
    )


class MarketStreamer:
    """
    Maintains an asynchronous WebSocket subscription with auto-reconnect.

    Live ``best_bid`` / ``best_ask`` from ``price_change`` (and full ``book`` snapshots)
    are written into :attr:`order_book`. **Silent trawler mode:** no stdout or spread
    alerts; only :class:`~polymarket_arb.shadow_executor.ShadowExecutor` banners should
    print during normal operation.
    """

    def __init__(
        self,
        *,
        settings: Settings,
        stream_slugs: Sequence[str],
        targets: Sequence[StreamTarget] | None = None,
        order_book: OrderBookState | None = None,
        spread_alert_threshold: Decimal = Decimal("0.02"),
        reconnect_base_seconds: float = 1.0,
        reconnect_max_seconds: float = 60.0,
        enable_custom_ws_features: bool = True,
    ) -> None:
        if not stream_slugs:
            raise ValueError("stream_slugs must contain at least one slug")
        self._settings = settings
        self._stream_slugs = tuple(str(s).strip() for s in stream_slugs if str(s).strip())
        if not self._stream_slugs:
            raise ValueError("stream_slugs must contain at least one non-empty slug")
        self._stream_label = "|".join(self._stream_slugs)
        self._preload_targets: Sequence[StreamTarget] | None = targets
        self.order_book = order_book if order_book is not None else OrderBookState()
        self.spread_alert_threshold = spread_alert_threshold
        self._reconnect_base = reconnect_base_seconds
        self._reconnect_max = reconnect_max_seconds
        self._custom = enable_custom_ws_features
        self._stop = asyncio.Event()
        self._fail_backoff = reconnect_base_seconds
        self._labels: dict[str, str] = {}
        self._asset_ids: list[str] = []
        self._subs_lock = asyncio.Lock()
        self._active_ws: websockets.WebSocketClientProtocol | None = None

    def stop(self) -> None:
        self._stop.set()

    async def update_subscriptions(self, new_slugs: list[str]) -> None:
        """
        Hot-swap target slugs and trigger a clean reconnect.

        ``run_forever`` will re-resolve asset IDs using the updated slug list on next loop.
        """

        updated = tuple(str(s).strip() for s in new_slugs if str(s).strip())
        if not updated:
            return
        async with self._subs_lock:
            if updated == self._stream_slugs:
                return
            self._stream_slugs = updated
            self._stream_label = "|".join(updated)
            self._preload_targets = None
            ws = self._active_ws
        if ws is not None and not ws.closed:
            await ws.close(code=1000, reason="subscription update")

    async def _ensure_targets(self, http: httpx.AsyncClient) -> None:
        async with self._subs_lock:
            active_slugs = self._stream_slugs
            preload_targets = self._preload_targets
            stream_label = self._stream_label
        if preload_targets is not None:
            targets = list(preload_targets)
        else:
            resolver = GammaSlugResolver(self._settings)
            targets = await resolver.resolve_targets_for_slugs(http, active_slugs)
        self._labels = {t.asset_id: t.outcome_label for t in targets}
        self._asset_ids = [t.asset_id for t in targets]
        logger.debug(
            "Resolved %d slug(s) %s → %d deduped CLOB asset id(s)",
            len(active_slugs),
            stream_label,
            len(self._asset_ids),
        )

    async def _maybe_spread_alert(self, _asset_id: str, _source_event: str) -> None:
        # Silent trawler: no ``ALERT spread=`` stdout; book still updates via handlers above.
        # (``format_spread_alert_line`` retained for optional tooling / tests.)
        pass

    def _handle_book_message(self, payload: Mapping[str, Any]) -> None:
        asset_id = str(payload.get("asset_id", ""))
        if not asset_id:
            return
        bids = payload.get("bids") or []
        asks = payload.get("asks") or []
        if not isinstance(bids, list) or not isinstance(asks, list):
            return
        top = _best_from_book_levels(bids, asks)
        if top is None:
            return
        bid, bid_size, ask, ask_size = top
        self.order_book.update_book(asset_id, bid, ask, bid_size, ask_size)

    def _apply_price_change_row(self, ch: Mapping[str, Any]) -> str | None:
        aid = str(ch.get("asset_id", ""))
        if not aid:
            return None
        bb = ch.get("best_bid")
        ba = ch.get("best_ask")
        bbs = ch.get("best_bid_size", ch.get("best_bid_quantity", 0))
        bas = ch.get("best_ask_size", ch.get("best_ask_quantity", 0))
        if bb is None or ba is None:
            return None
        if isinstance(bb, str) and bb.strip() == "":
            return None
        if isinstance(ba, str) and ba.strip() == "":
            return None
        self.order_book.update_book(aid, bb, ba, bbs, bas)
        return aid

    def _handle_price_change(self, payload: Mapping[str, Any]) -> list[str]:
        updated: list[str] = []
        changes = payload.get("price_changes") or []
        if not isinstance(changes, list):
            return updated
        for ch in changes:
            if not isinstance(ch, Mapping):
                continue
            aid = self._apply_price_change_row(ch)
            if aid:
                updated.append(aid)
        return updated

    def _handle_best_bid_ask(self, payload: Mapping[str, Any]) -> str | None:
        aid = str(payload.get("asset_id", ""))
        if not aid:
            return None
        bb = payload.get("best_bid")
        ba = payload.get("best_ask")
        bbs = payload.get("best_bid_size", payload.get("best_bid_quantity", 0))
        bas = payload.get("best_ask_size", payload.get("best_ask_quantity", 0))
        if bb is None or ba is None:
            return None
        self.order_book.update_book(aid, bb, ba, bbs, bas)
        return aid

    async def _handle_payload(self, payload: Any) -> None:
        if not isinstance(payload, Mapping):
            return
        event_type = str(payload.get("event_type", ""))
        if event_type == "book":
            self._handle_book_message(payload)
            asset_id = str(payload.get("asset_id", ""))
            if asset_id:
                self._fail_backoff = self._reconnect_base
                await self._maybe_spread_alert(asset_id, event_type)
        elif event_type == "price_change":
            touched = self._handle_price_change(payload)
            if touched:
                self._fail_backoff = self._reconnect_base
                for asset_id in sorted(set(touched)):
                    await self._maybe_spread_alert(asset_id, event_type)
        elif event_type == "best_bid_ask":
            aid = self._handle_best_bid_ask(payload)
            if aid:
                self._fail_backoff = self._reconnect_base
                await self._maybe_spread_alert(aid, event_type)

    async def _stream_session(self, http: httpx.AsyncClient) -> None:
        await self._ensure_targets(http)
        self._fail_backoff = self._reconnect_base
        self.order_book.discard_tokens(self._asset_ids)
        subscribe = {
            "assets_ids": self._asset_ids,
            "type": "market",
            "custom_feature_enabled": self._custom,
        }
        raw = json.dumps(subscribe)
        logger.debug("Subscribing with %d asset id(s)", len(self._asset_ids))
        async with websockets.connect(
            self._settings.clob_market_ws_url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
        ) as ws:
            self._active_ws = ws
            try:
                await ws.send(raw)
                async for message in ws:
                    if self._stop.is_set():
                        await ws.close()
                        return
                    raw_txt = message.decode() if isinstance(message, bytes) else str(message)
                    try:
                        payload = json.loads(raw_txt)
                    except json.JSONDecodeError:
                        logger.debug("Non-JSON WS message: %s", raw_txt[:200])
                        continue
                    if isinstance(payload, list):
                        for item in payload:
                            await self._handle_payload(item)
                    else:
                        await self._handle_payload(payload)
            finally:
                self._active_ws = None

    async def run_forever(self) -> None:
        """
        Block until :meth:`stop` is called, reconnecting with exponential backoff.
        """

        timeout = httpx.Timeout(self._settings.request_timeout_seconds)
        headers = {"User-Agent": self._settings.http_user_agent}
        async with httpx.AsyncClient(timeout=timeout, headers=headers) as http:
            while not self._stop.is_set():
                try:
                    await self._stream_session(http)
                    if self._stop.is_set():
                        break
                    logger.debug(
                        "Polymarket market WebSocket session ended; reconnecting in %.1fs",
                        self._fail_backoff,
                    )
                except (ConnectionClosed, OSError, httpx.HTTPError, websockets.InvalidURI) as exc:
                    logger.debug("Market stream connection error: %s", exc)
                except Exception:
                    logger.debug("Unexpected error in market stream loop", exc_info=True)
                if self._stop.is_set():
                    break
                await asyncio.sleep(self._fail_backoff)
                self._fail_backoff = min(self._fail_backoff * 2.0, self._reconnect_max)


async def run_default_eth_event_stream(settings: Settings | None = None) -> None:
    """Convenience entry: legacy Ethereum March event (single slug)."""

    settings = settings or Settings.from_env()
    streamer = MarketStreamer(
        settings=settings,
        stream_slugs=("what-price-will-ethereum-hit-in-march-2026",),
        spread_alert_threshold=Decimal("0.02"),
    )
    await streamer.run_forever()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_default_eth_event_stream())
