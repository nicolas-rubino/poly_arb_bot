"""
Latency-aware \"ghost\" execution drills against a shared :class:`~polymarket_arb.orderbook.OrderBookState`.

``run_latency_calibration_loop`` continues to probe the CLOB for long-term RTT metrics. Each
:meth:`attempt_ghost_trade` simulates a fixed **12 ms** delay (stand‑in for ultra‑low latency AWS
EC2 to the CLOB) before re-read, and accrues :attr:`session_pnl` / :attr:`successful_trades` on
successful legs.
"""

from __future__ import annotations

import asyncio
import csv
import logging
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import httpx

from polymarket_arb.clients.clob import measure_clob_health_latency
from polymarket_arb.config import Settings
from polymarket_arb.orderbook import OrderBookState

logger = logging.getLogger(__name__)

# ANSI SGR — works on most modern terminals (including Windows 10+ VT mode).
_STYLE_GREEN_BOLD = "\033[92;1m"
_STYLE_RED_BOLD = "\033[91;1m"
_STYLE_RESET = "\033[0m"


def _append_csv_row_sync(path: Path, row: list[str]) -> None:
    """Blocking CSV append (run via :func:`asyncio.to_thread`)."""

    with open(path, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)


def _ensure_csv_headers_sync(path: Path) -> None:
    """Create ``session_stats.csv`` with headers when missing or empty."""

    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.stat().st_size == 0:
        with open(path, "w", newline="", encoding="utf-8") as f:
            f.write("timestamp,market_id,token_id,profit,total_pnl\n")


def _massive_banner(lines: list[str], *, ok: bool) -> None:
    style = _STYLE_GREEN_BOLD if ok else _STYLE_RED_BOLD
    bar = "=" * 80
    body = "\n".join(lines)
    print(f"\n\n{style}{bar}\n{body}\n{bar}{_STYLE_RESET}\n", flush=True)


class ShadowExecutor:
    """
    Depends on :class:`~polymarket_arb.orderbook.OrderBookState` for post-delay book checks.

    Runs :meth:`run_latency_calibration_loop` as a concurrent task: calibrate immediately,
    then refresh ``current_latency_ms`` every ``calibration_interval_seconds`` (default 1 hour).
    """

    def __init__(
        self,
        *,
        settings: Settings,
        order_book: OrderBookState,
        calibration_interval_seconds: float = 3600.0,
        session_stats_csv: str | Path = "session_stats.csv",
    ) -> None:
        self._settings = settings
        self._order_book = order_book
        self.calibration_interval_seconds = max(calibration_interval_seconds, 1.0)
        self._csv_path = Path(session_stats_csv)
        self.current_latency_ms: float = 0.0
        self.session_pnl = Decimal("0")
        self.successful_trades = 0
        self._stop = asyncio.Event()
        self._cal_lock = asyncio.Lock()
        self._stats_lock = asyncio.Lock()
        self._csv_lock = asyncio.Lock()
        _ensure_csv_headers_sync(self._csv_path)

    def stop(self) -> None:
        self._stop.set()

    async def _calibrate_latency(self) -> None:
        """Measure RTT (ms) for ``GET {clob_base}/`` and store in :attr:`current_latency_ms`."""

        async with self._cal_lock:
            timeout = httpx.Timeout(self._settings.request_timeout_seconds)
            headers = {"User-Agent": self._settings.http_user_agent}
            async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
                result = await measure_clob_health_latency(client, self._settings)
            if result.http_status >= 400:
                logger.debug(
                    "ShadowExecutor calibration: HTTP %s (keeping previous latency_ms=%.2f)",
                    result.http_status,
                    self.current_latency_ms,
                )
                return
            self.current_latency_ms = result.latency_ms
            logger.debug(
                "ShadowExecutor calibrated CLOB RTT: %.2f ms (status=%s)",
                self.current_latency_ms,
                result.http_status,
            )

    async def run_latency_calibration_loop(self) -> None:
        """
        Calibrate once, then repeat on the configured interval until :meth:`stop`.
        """

        while not self._stop.is_set():
            try:
                await self._calibrate_latency()
            except Exception:
                logger.debug("ShadowExecutor calibration failed", exc_info=True)
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self.calibration_interval_seconds,
                )
                break
            except asyncio.TimeoutError:
                continue

    async def attempt_ghost_trade(
        self,
        *,
        market_id: str,
        yes_token_id: str,
        no_token_id: str,
        yes_target_price: Decimal | str | float,
        no_target_price: Decimal | str | float,
        paper_probe_ms: int,
        paper_second_leg_probe_ms: int,
        package_cost_usd: Decimal,
        max_combined_ask: Decimal,
    ) -> None:
        """
        Sequential two-leg paper fill simulation:
        leg 1 checks after ``paper_probe_ms`` and leg 2 checks after
        ``paper_second_leg_probe_ms``. A package win is recorded only if both survive.
        """

        t0 = time.perf_counter()
        yes_target = Decimal(str(yes_target_price).strip())
        no_target = Decimal(str(no_target_price).strip())
        await asyncio.sleep(max(float(paper_probe_ms), 0.0) / 1000.0)

        yes_quote = self._order_book.get_quote(str(yes_token_id))
        elapsed_ms_leg1 = (time.perf_counter() - t0) * 1000.0
        if yes_quote is None:
            _massive_banner(
                [
                    "SHADOW FAILED: No quote for leg 1 after probe.",
                    f"  market_id={market_id}",
                    f"  token_id_yes={yes_token_id}",
                    f"  T_0→leg1 wall: {elapsed_ms_leg1:.2f} ms",
                ],
                ok=False,
            )
            return
        yes_ask = yes_quote.best_ask
        if yes_ask > yes_target:
            _massive_banner(
                [
                    "SHADOW FAILED: Leg 1 price moved away.",
                    f"  market_id={market_id}",
                    f"  token_id_yes={yes_token_id}",
                    f"  target_price={yes_target}  best_ask_after={yes_ask}",
                    f"  T_0→leg1 wall: {elapsed_ms_leg1:.2f} ms",
                ],
                ok=False,
            )
            return

        await asyncio.sleep(max(float(paper_second_leg_probe_ms), 0.0) / 1000.0)
        no_quote = self._order_book.get_quote(str(no_token_id))
        elapsed_ms_total = (time.perf_counter() - t0) * 1000.0
        if no_quote is None:
            _massive_banner(
                [
                    "SHADOW FAILED: No quote for leg 2 after second probe.",
                    f"  market_id={market_id}",
                    f"  token_id_no={no_token_id}",
                    f"  T_0→leg2 wall: {elapsed_ms_total:.2f} ms",
                ],
                ok=False,
            )
            return
        no_ask = no_quote.best_ask
        if no_ask > no_target:
            _massive_banner(
                [
                    "SHADOW FAILED: Leg 2 price moved away.",
                    f"  market_id={market_id}",
                    f"  token_id_no={no_token_id}",
                    f"  target_price={no_target}  best_ask_after={no_ask}",
                    f"  T_0→leg2 wall: {elapsed_ms_total:.2f} ms",
                ],
                ok=False,
            )
            return

        package_profit = Decimal(package_cost_usd) * (Decimal(1) - Decimal(max_combined_ask))
        async with self._stats_lock:
            self.session_pnl += package_profit
            self.successful_trades += 1
            pnl_show = self.session_pnl
            wins_show = self.successful_trades
        timestamp = datetime.now(timezone.utc).isoformat()
        row = [
            timestamp,
            market_id,
            f"{yes_token_id}|{no_token_id}",
            str(package_profit),
            str(pnl_show),
        ]
        try:
            async with self._csv_lock:
                await asyncio.to_thread(_append_csv_row_sync, self._csv_path, row)
        except OSError:
            logger.debug("session_stats.csv append failed (disk error)", exc_info=True)
        _massive_banner(
            [
                "SHADOW SUCCESS: Sequential package survived both probes.",
                f"  market_id={market_id}",
                f"  leg1 yes target={yes_target} ask_after={yes_ask}",
                f"  leg2 no  target={no_target} ask_after={no_ask}",
                f"  probe_ms={paper_probe_ms} second_probe_ms={paper_second_leg_probe_ms}",
                f"  package_profit={package_profit}",
                f"  T_0→leg2 wall: {elapsed_ms_total:.2f} ms",
                "",
                f"TOTAL GHOST PnL: +${pnl_show} | WINS: {wins_show}",
            ],
            ok=True,
        )
