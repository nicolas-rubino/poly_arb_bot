"""
Centralized in-memory top-of-book quotes for many CLOB token IDs.

Designed for sub-millisecond updates: a single dict write per ``update_book`` call.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal


def _to_decimal(value: Decimal | str | float | int) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value).strip())


@dataclass(frozen=True, slots=True)
class TokenQuote:
    """Immutable snapshot of best bid / ask for one outcome token."""

    token_id: str
    best_bid: Decimal
    best_ask: Decimal
    best_bid_size: Decimal
    best_ask_size: Decimal

    @property
    def spread(self) -> Decimal:
        return self.best_ask - self.best_bid


class OrderBookState:
    """
    In-memory registry of best bid/ask and top size per token.

    Threading: a single asyncio task should call :meth:`update_book`; for multi-threaded
    access, protect with a lock externally.
    """

    __slots__ = ("_quotes",)

    def __init__(self) -> None:
        self._quotes: dict[str, tuple[Decimal, Decimal, Decimal, Decimal]] = {}

    def update_book(
        self,
        token_id: str,
        bid: Decimal | str | float | int,
        ask: Decimal | str | float | int,
        bid_size: Decimal | str | float | int = Decimal(0),
        ask_size: Decimal | str | float | int = Decimal(0),
    ) -> None:
        """Replace best bid/ask for ``token_id`` (O(1) dict update)."""
        tid = str(token_id)
        self._quotes[tid] = (
            _to_decimal(bid),
            _to_decimal(ask),
            _to_decimal(bid_size),
            _to_decimal(ask_size),
        )

    def get_quote(self, token_id: str) -> TokenQuote | None:
        """Return a frozen quote for ``token_id``, or ``None`` if unknown."""
        row = self._quotes.get(str(token_id))
        if row is None:
            return None
        bid, ask, bid_size, ask_size = row
        return TokenQuote(
            token_id=str(token_id),
            best_bid=bid,
            best_ask=ask,
            best_bid_size=bid_size,
            best_ask_size=ask_size,
        )

    def get_best_bid(self, token_id: str) -> Decimal | None:
        row = self._quotes.get(str(token_id))
        return row[0] if row else None

    def get_best_ask(self, token_id: str) -> Decimal | None:
        row = self._quotes.get(str(token_id))
        return row[1] if row else None

    def get_spread(self, token_id: str) -> Decimal | None:
        """
        Return ``best_ask - best_bid`` for ``token_id``.

        ``None`` if the token has not been updated yet.
        """
        row = self._quotes.get(str(token_id))
        if row is None:
            return None
        bid, ask = row
        return ask - bid

    def token_ids(self) -> frozenset[str]:
        """All token IDs currently held."""
        return frozenset(self._quotes.keys())

    def discard_tokens(self, token_ids: Iterable[str]) -> None:
        """Remove quotes for the given token IDs (e.g. before a WebSocket reconnect)."""
        for tid in token_ids:
            self._quotes.pop(str(tid), None)

    def __len__(self) -> int:
        return len(self._quotes)
