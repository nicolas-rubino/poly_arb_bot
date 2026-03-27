"""
Cross-outcome (Yes/No) arbitrage scanning against a shared :class:`~polymarket_arb.orderbook.OrderBookState`.

Fetches per-token fee parameters from the public CLOB ``GET /fee-rate`` endpoint, applies the
official taker-fee curve (see Polymarket trading fees docs), and alerts when the all-in cost
to buy one unit of Yes at ask plus one unit of No at ask (plus estimated fees) beats
``1 - target_profit_margin``.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
from collections.abc import Iterable
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import httpx

from polymarket_arb.config import Settings
from polymarket_arb.orderbook import OrderBookState
from polymarket_arb.shadow_executor import ShadowExecutor

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AutoResearchConfig:
    PACKAGE_COST_USD: Decimal = Decimal("8.00")
    MAX_COMBINED_ASK: Decimal = Decimal("0.990")
    MIN_GROSS_EDGE: Decimal = Decimal("0.008")
    MAX_SIDE_SPREAD: Decimal = Decimal("0.03")
    MIN_SIDE_DEPTH: Decimal = Decimal("20.00")
    MIN_TOP_ASK_SIZE: Decimal = Decimal("8.00")
    ENTRY_START_SEC: int = 120
    ENTRY_CUTOFF_SEC: int = 60
    COOLDOWN_MS: int = 4000
    PAPER_PROBE_MS: int = 1500
    PAPER_SECOND_LEG_PROBE_MS: int = 900


AUTORESEARCH_CONFIG = AutoResearchConfig()


def _parse_json_list_field(value: Any) -> Any:
    if isinstance(value, str) and value:
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


@dataclass(frozen=True, slots=True)
class TokenFeeParams:
    """Cached fee inputs for the Polymarket taker fee curve."""

    token_id: str
    base_fee_bps: int
    fee_rate: Decimal
    exponent: Decimal


@dataclass(frozen=True, slots=True)
class YesNoMarket:
    """One binary market: outcome token IDs for Yes and No."""

    condition_id: str
    yes_token_id: str
    no_token_id: str
    label: str
    end_date_time: dt.datetime


def _to_decimal_or(value: Any, default: Decimal) -> Decimal:
    if value is None or value == "":
        return default
    return Decimal(str(value).strip())


def _parse_gamma_datetime(value: Any) -> dt.datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def parse_fee_rate_response(token_id: str, payload: dict[str, Any]) -> TokenFeeParams:
    """
    Map CLOB ``/fee-rate`` JSON into curve parameters.

    The public schema documents ``base_fee`` (int, basis points). If the API adds
    ``fee_rate`` / ``feeRate`` and ``exponent``, those are preferred for the curve
    formula; otherwise ``fee_rate`` defaults to ``base_fee / 10000`` and ``exponent``
    defaults to ``1`` (per current docs when only bps is available).
    """

    bps = int(payload.get("base_fee", 0) or 0)
    explicit_rate = payload.get("fee_rate", payload.get("feeRate"))
    if explicit_rate is not None and str(explicit_rate).strip() != "":
        fee_rate = _to_decimal_or(explicit_rate, Decimal(0))
    else:
        fee_rate = Decimal(bps) / Decimal(10000)
    explicit_exp = payload.get("exponent")
    if explicit_exp is not None and str(explicit_exp).strip() != "":
        exponent = _to_decimal_or(explicit_exp, Decimal(1))
    else:
        exponent = Decimal(1)
    return TokenFeeParams(
        token_id=str(token_id),
        base_fee_bps=bps,
        fee_rate=fee_rate,
        exponent=exponent,
    )


def calculate_taker_fee_usdc(
    size: Decimal,
    price: Decimal,
    params: TokenFeeParams,
) -> Decimal:
    """
    ``fee = size * price * feeRate * (price * (1 - price)) ** exponent``

    Rounds to 4 decimal places (Polymarket fee precision). Sizes/prices outside ``(0,1)``
    for the probability still evaluate; extreme prices may round to zero fee.
    """

    if size <= 0 or params.fee_rate <= 0:
        return Decimal(0).quantize(Decimal("0.0001"))
    p = price
    one = Decimal(1)
    inner = p * (one - p)
    if inner <= 0:
        curve = Decimal(0)
    else:
        curve = inner ** params.exponent
    raw = size * p * params.fee_rate * curve
    return raw.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


async def fetch_yes_no_markets(
    client: httpx.AsyncClient,
    settings: Settings,
    event_slug: str,
) -> list[YesNoMarket]:
    """Resolve a Gamma *event* slug into Yes/No token pairs (one row per strike/market)."""

    headers = {"User-Agent": settings.http_user_agent}
    url = f"{settings.gamma_api_base_url}/events"
    resp = await client.get(url, params={"slug": event_slug}, headers=headers)
    resp.raise_for_status()
    events = resp.json()
    if not isinstance(events, list) or not events:
        raise ValueError(f"No Gamma event for slug={event_slug!r}")
    event = events[0]
    markets_raw = event.get("markets") or []
    if not isinstance(markets_raw, list):
        return []
    pairs: list[YesNoMarket] = []
    for m in markets_raw:
        if not m.get("enableOrderBook", False):
            continue
        raw_ids = m.get("clobTokenIds")
        if isinstance(raw_ids, str):
            ids = [str(x) for x in json.loads(raw_ids)]
        elif isinstance(raw_ids, list):
            ids = [str(x) for x in raw_ids]
        else:
            continue
        outcomes = _parse_json_list_field(m.get("outcomes")) or []
        if not isinstance(outcomes, list) or len(ids) != len(outcomes):
            continue
        idx_yes: int | None = None
        idx_no: int | None = None
        for i, o in enumerate(outcomes):
            label = str(o).strip().lower()
            if label == "yes":
                idx_yes = i
            elif label == "no":
                idx_no = i
        if idx_yes is None or idx_no is None or idx_yes == idx_no:
            continue
        cond = str(m.get("conditionId") or m.get("id") or "")
        title = str(m.get("groupItemTitle") or m.get("question") or cond or "market")
        end_date_time = _parse_gamma_datetime(
            m.get("endDate") or m.get("resolveDate") or event.get("endDate") or event.get("resolveDate"),
        )
        if end_date_time is None:
            continue
        pairs.append(
            YesNoMarket(
                condition_id=cond or f"{title}:{idx_yes}:{idx_no}",
                yes_token_id=ids[idx_yes],
                no_token_id=ids[idx_no],
                label=title,
                end_date_time=end_date_time,
            )
        )
    if not pairs:
        raise ValueError(f"No Yes/No CLOB pairs parsed for event slug={event_slug!r}")
    return pairs


class ArbitrageScanner:
    """
    Depends on a shared :class:`~polymarket_arb.orderbook.OrderBookState` mutated by the streamer.

    Call :meth:`initialize` once, then run :meth:`scan_for_opportunities` concurrently with the
    market WebSocket. Use :meth:`run_fee_refresh_loop` as a third concurrent task (refreshes
    every ``fee_refresh_hours``).
    """

    def __init__(
        self,
        *,
        settings: Settings,
        order_book: OrderBookState,
        markets: Sequence[YesNoMarket],
        target_profit_margin: Decimal = Decimal("0.01"),
        unit_size: Decimal = Decimal("1"),
        scan_interval_seconds: float = 0.05,
        fee_refresh_hours: float = 24.0,
        shadow_executor: ShadowExecutor | None = None,
    ) -> None:
        self._settings = settings
        self._order_book = order_book
        self._markets = list(markets)
        self._shadow = shadow_executor
        self.target_profit_margin = target_profit_margin
        self.unit_size = unit_size
        self.scan_interval_seconds = scan_interval_seconds
        self.fee_refresh_hours = fee_refresh_hours
        self._markets_lock = asyncio.Lock()
        self._fee_lock = asyncio.Lock()
        self._fees: dict[str, TokenFeeParams] = {}
        self._stop = asyncio.Event()
        self._last_alert_fp: dict[str, tuple[Decimal, Decimal, Decimal, Decimal, Decimal, Decimal]] = {}

    def stop(self) -> None:
        self._stop.set()

    def _fee_url(self, token_id: str) -> str:
        from urllib.parse import quote

        q = quote(str(token_id), safe="")
        return f"{self._settings.clob_base_url}/fee-rate?token_id={q}"

    async def _fetch_fee_params(self, client: httpx.AsyncClient, token_id: str) -> TokenFeeParams:
        url = self._fee_url(token_id)
        headers = {"User-Agent": self._settings.http_user_agent}
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise ValueError(f"Unexpected fee-rate payload for {token_id}: {data!r}")
        return parse_fee_rate_response(token_id, data)

    @staticmethod
    def _token_ids_for_markets(markets: Iterable[YesNoMarket]) -> set[str]:
        ids: set[str] = set()
        for market in markets:
            ids.add(market.yes_token_id)
            ids.add(market.no_token_id)
        return ids

    async def refresh_fee_cache(
        self,
        client: httpx.AsyncClient | None = None,
        token_ids: set[str] | None = None,
    ) -> None:
        """Fetch ``/fee-rate`` for every token ID referenced by configured Yes/No markets."""

        if token_ids is None:
            async with self._markets_lock:
                ids = self._token_ids_for_markets(self._markets)
        else:
            ids = set(token_ids)
        close = False
        if client is None:
            timeout = httpx.Timeout(self._settings.request_timeout_seconds)
            client = httpx.AsyncClient(timeout=timeout, headers={"User-Agent": self._settings.http_user_agent})
            close = True
        try:
            new_cache: dict[str, TokenFeeParams] = {}
            for tid in sorted(ids):
                try:
                    new_cache[tid] = await self._fetch_fee_params(client, tid)
                except httpx.HTTPError as exc:
                    logger.debug("fee-rate fetch failed for %s: %s", tid, exc)
            async with self._fee_lock:
                self._fees.update(new_cache)
            logger.debug("Fee cache refreshed for %d token id(s)", len(new_cache))
        finally:
            if close:
                await client.aclose()

    async def update_markets(self, new_markets: list[YesNoMarket]) -> None:
        """
        Hot-swap tracked markets and eagerly refresh fee cache for their token IDs.

        This method is safe to call while :meth:`scan_for_opportunities` is running.
        """

        deduped: list[YesNoMarket] = []
        seen: set[str] = set()
        for market in new_markets:
            key = (market.condition_id or "").strip() or f"{market.yes_token_id}|{market.no_token_id}"
            if key in seen:
                continue
            seen.add(key)
            deduped.append(market)
        token_ids = self._token_ids_for_markets(deduped)
        async with self._markets_lock:
            self._markets = deduped
            self._last_alert_fp = {
                key: value for key, value in self._last_alert_fp.items() if key in seen
            }
        if token_ids:
            await self.refresh_fee_cache(token_ids=token_ids)

    async def initialize(self) -> None:
        """Async setup: load fee parameters for all tracked tokens."""

        await self.refresh_fee_cache()

    async def run_fee_refresh_loop(self) -> None:
        """Background task: re-fetch fee cache every ``fee_refresh_hours``."""

        interval = max(float(self.fee_refresh_hours), 1.0) * 3600.0
        while True:
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
                break
            except asyncio.TimeoutError:
                try:
                    await self.refresh_fee_cache()
                except Exception:
                    logger.debug("fee refresh loop error", exc_info=True)
            except asyncio.CancelledError:
                break

    async def _fee_for(self, token_id: str) -> TokenFeeParams:
        async with self._fee_lock:
            cached = self._fees.get(str(token_id))
        if cached is not None:
            return cached
        return TokenFeeParams(
            token_id=str(token_id),
            base_fee_bps=0,
            fee_rate=Decimal(0),
            exponent=Decimal(1),
        )

    async def scan_for_opportunities(self) -> None:
        """
        Hot loop: for each Yes/No pair, compute all-in buy cost for ``unit_size`` on each leg.

        Alerts when ``ask_yes + ask_no + fee_yes + fee_no < 1 - target_profit_margin``.
        """

        cfg = AUTORESEARCH_CONFIG
        unit = self.unit_size
        while not self._stop.is_set():
            async with self._markets_lock:
                markets = list(self._markets)
            for market in markets:
                if self._stop.is_set():
                    return
                qy = self._order_book.get_quote(market.yes_token_id)
                qn = self._order_book.get_quote(market.no_token_id)
                if qy is None or qn is None:
                    continue
                now_utc = dt.datetime.now(tz=dt.timezone.utc)
                seconds_to_expiry = (market.end_date_time - now_utc).total_seconds()
                if seconds_to_expiry < float(cfg.ENTRY_CUTOFF_SEC):
                    continue
                if seconds_to_expiry > float(cfg.ENTRY_START_SEC):
                    continue
                if qy.spread > cfg.MAX_SIDE_SPREAD or qn.spread > cfg.MAX_SIDE_SPREAD:
                    continue
                if qy.best_ask_size < cfg.MIN_TOP_ASK_SIZE or qn.best_ask_size < cfg.MIN_TOP_ASK_SIZE:
                    continue
                if qy.best_bid_size < cfg.MIN_SIDE_DEPTH or qn.best_bid_size < cfg.MIN_SIDE_DEPTH:
                    continue
                ask_yes = qy.best_ask
                ask_no = qn.best_ask
                if ask_yes <= 0 or ask_no <= 0:
                    continue
                fy = await self._fee_for(market.yes_token_id)
                fn = await self._fee_for(market.no_token_id)
                fee_yes = calculate_taker_fee_usdc(unit, ask_yes, fy)
                fee_no = calculate_taker_fee_usdc(unit, ask_no, fn)
                total = ask_yes + ask_no + fee_yes + fee_no
                net = Decimal(1) - total
                if total <= cfg.MAX_COMBINED_ASK and net >= cfg.MIN_GROSS_EDGE:
                    fp = (ask_yes, ask_no, fee_yes, fee_no, total, net)
                    if self._last_alert_fp.get(market.condition_id) == fp:
                        continue
                    self._last_alert_fp[market.condition_id] = fp
                    # Silent trawler: no arb banner on stdout; ShadowExecutor prints ghost outcomes only.
                    if self._shadow is not None:
                        mid = market.condition_id
                        asyncio.create_task(
                            self._shadow.attempt_ghost_trade(
                                market_id=mid,
                                yes_token_id=market.yes_token_id,
                                no_token_id=market.no_token_id,
                                yes_target_price=ask_yes,
                                no_target_price=ask_no,
                                paper_probe_ms=cfg.PAPER_PROBE_MS,
                                paper_second_leg_probe_ms=cfg.PAPER_SECOND_LEG_PROBE_MS,
                                package_cost_usd=cfg.PACKAGE_COST_USD,
                                max_combined_ask=cfg.MAX_COMBINED_ASK,
                            ),
                            name=f"shadow_pkg_{mid[:12]}",
                        )
                        await asyncio.sleep(cfg.COOLDOWN_MS / 1000.0)
            await asyncio.sleep(self.scan_interval_seconds)
