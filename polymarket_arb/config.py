from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value.strip() if value else default


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime configuration (immutable). Override via environment variables."""

    clob_base_url: str
    gamma_api_base_url: str
    clob_market_ws_url: str
    http_user_agent: str
    request_timeout_seconds: float

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            clob_base_url=_env(
                "POLYMARKET_CLOB_BASE_URL",
                "https://clob.polymarket.com",
            ).rstrip("/"),
            gamma_api_base_url=_env(
                "POLYMARKET_GAMMA_API_BASE_URL",
                "https://gamma-api.polymarket.com",
            ).rstrip("/"),
            clob_market_ws_url=_env(
                "POLYMARKET_CLOB_MARKET_WS_URL",
                "wss://ws-subscriptions-clob.polymarket.com/ws/market",
            ),
            http_user_agent=_env(
                "POLYMARKET_HTTP_USER_AGENT",
                "polymarket-arb/0.1 (+https://github.com/polymarket)",
            ),
            request_timeout_seconds=float(_env("POLYMARKET_HTTP_TIMEOUT", "15")),
        )
