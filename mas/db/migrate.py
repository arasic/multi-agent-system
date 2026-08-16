"""Plain-SQL migrations: mas/db/migrations/NNNN_name.sql, applied in order, tracked in schema_migrations."""

from __future__ import annotations

import logging
from importlib import resources

from mas.db.connection import Conn

log = logging.getLogger(__name__)

_BOOTSTRAP = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);
"""


def _migration_files() -> list[tuple[str, str]]:
    root = resources.files("mas.db").joinpath("migrations")
    files = sorted(p for p in root.iterdir() if p.name.endswith(".sql"))
    return [(p.name, p.read_text(encoding="utf-8")) for p in files]


def migrate(conn: Conn) -> list[str]:
    """Apply pending migrations. Idempotent. Returns names applied this call."""
    applied: list[str] = []
    with conn.transaction():
        conn.execute(_BOOTSTRAP)
        conn.execute("LOCK TABLE schema_migrations IN EXCLUSIVE MODE")
        done = {r["version"] for r in conn.execute("SELECT version FROM schema_migrations").fetchall()}
        for name, sql in _migration_files():
            if name in done:
                continue
            log.info("applying migration %s", name)
            conn.execute(sql)
            conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (name,))
            applied.append(name)
    return applied
