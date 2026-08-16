"""Database access. Postgres is both the blackboard and the coordination mechanism (ADR-005).

No LLM calls here, ever (invariant I-1).
"""

from mas.db.connection import connect, dict_row, transaction
from mas.db.migrate import migrate

__all__ = ["connect", "dict_row", "migrate", "transaction"]
