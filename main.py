"""
Polymarket arbitrage bot — async entrypoint.

Run (WSL2 / Linux / macOS / Windows):
  cd polymarket-arb-bot && python -m venv .venv && source .venv/bin/activate
  pip install -e .
  python main.py

``main`` runs :func:`~polymarket_arb.app.run`, which starts a multi-slug market WebSocket, the
fee-cache refresh task, :class:`~polymarket_arb.shadow_executor.ShadowExecutor` latency
calibration, and :class:`~polymarket_arb.arbitrage_engine.ArbitrageScanner` (minimal margin) via
:func:`asyncio.gather` inside the app layer.
"""

from __future__ import annotations

import asyncio
import logging
import sys

from polymarket_arb.app import run

logger = logging.getLogger(__name__)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )


def main() -> None:
    _configure_logging()
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Stopped by user (KeyboardInterrupt).")


if __name__ == "__main__":
    main()
