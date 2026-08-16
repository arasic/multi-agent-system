"""psycopg3 connection helpers. One connection per actor (orchestrator loop, worker loop); no pool yet."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from mas.config import settings

__all__ = ["Jsonb", "connect", "dict_row", "transaction", "Conn"]

Conn = psycopg.Connection[dict[str, Any]]


def connect(database_url: str | None = None, *, autocommit: bool = True) -> Conn:
    """Open a connection with dict rows. Caller owns the connection.

    Autocommit by default: every atomic unit of work is an explicit `with conn.transaction():` block.
    (With autocommit=False a bare SELECT opens an implicit transaction that never commits, and later
    `transaction()` blocks silently become savepoints inside it — writes are invisible to other connections.)
    """
    url = database_url or settings().database_url
    return psycopg.connect(url, row_factory=dict_row, autocommit=autocommit)


@contextmanager
def transaction(conn: Conn) -> Iterator[Conn]:
    """Explicit transaction block. Commits on success, rolls back on exception."""
    with conn.transaction():
        yield conn
