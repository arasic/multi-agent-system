"""Schema + DB-enforced invariants: migrations idempotent, artifacts immutable (I-5), transitions guarded (I-2)."""

import psycopg
import pytest

from mas.artifacts import store
from mas.db import migrate
from mas.models.enums import ArtifactStatus, RunStatus, TaskStatus
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import state_machine as sm
from mas.orchestrator.state_machine import IllegalTransition
from tests.conftest import CAPS, diamond

pytestmark = pytest.mark.db


def test_migrate_is_idempotent(conn):
    assert migrate(conn) == []  # session fixture already applied
    tables = {
        r["table_name"] for r in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    }
    assert {"runs", "tasks", "task_dependencies", "attempts", "artifacts", "events", "schema_migrations"} <= tables


def test_install_dag_creates_tasks_and_dependencies(conn):
    run = runs_mod.create_run_from_dag(conn, diamond(), capabilities=set(CAPS))
    assert run.status is RunStatus.RUNNING
    assert run.tasks_created == 5
    tasks = {t.key: t for t in sm.tasks_for_run(conn, run.id)}
    assert set(tasks) == {"T1", "T2", "T3", "T4", "T5"}
    assert all(t.status is TaskStatus.PENDING for t in tasks.values())
    n_deps = conn.execute("SELECT count(*) AS n FROM task_dependencies").fetchone()["n"]
    assert n_deps == 6  # T2,T3,T4→T1 (3) + T5→T2,T3,T4 (3)
    types = [e["type"] for e in conn.execute("SELECT type FROM events WHERE run_id=%s ORDER BY id", (run.id,))]
    assert types[:3] == ["run.created", "run.planning", "plan.validated"]
    assert types[-1] == "run.running"


def test_invalid_dag_fails_run_with_verdict(conn):
    d = diamond()
    d.by_id()["T2"].depends_on.append("nope")
    with pytest.raises(runs_mod.InvalidDag):
        runs_mod.create_run_from_dag(conn, d, capabilities=set(CAPS))
    run = conn.execute("SELECT status, verdict FROM runs").fetchone()
    assert run["status"] == "FAILED"
    assert run["verdict"].startswith("FAIL:invalid plan")


def test_artifact_content_is_immutable(conn):
    run = runs_mod.create_run_from_dag(conn, diamond(), capabilities=set(CAPS))
    with conn.transaction():
        a = store.publish(conn, run_id=run.id, type="document", ref="stub:x", meta={"name": "x"})
    with pytest.raises(psycopg.errors.RaiseException):
        with conn.transaction():
            conn.execute("UPDATE artifacts SET ref = 'tampered' WHERE id = %s", (a.id,))
    with pytest.raises(psycopg.errors.RaiseException):
        with conn.transaction():
            conn.execute("UPDATE artifacts SET meta = '{}'::jsonb WHERE id = %s", (a.id,))
    with pytest.raises(psycopg.errors.RaiseException):
        with conn.transaction():
            conn.execute("DELETE FROM artifacts WHERE id = %s", (a.id,))
    # status may change, via the state machine
    with conn.transaction():
        acc = store.accept(conn, a.id)
    assert acc.status is ArtifactStatus.ACCEPTED
    with conn.transaction():
        b = store.publish(conn, run_id=run.id, type="document", ref="stub:y")
        sup = store.supersede(conn, a.id, b.id)
    assert sup.status is ArtifactStatus.SUPERSEDED and sup.superseded_by == b.id
    with pytest.raises(IllegalTransition):
        with conn.transaction():
            store.accept(conn, a.id)  # superseded → accepted is illegal


def test_illegal_transition_is_rejected_and_leaves_status_unchanged(conn):
    run = runs_mod.create_run_from_dag(conn, diamond(), capabilities=set(CAPS))
    t1 = next(t for t in sm.tasks_for_run(conn, run.id) if t.key == "T1")
    with pytest.raises(IllegalTransition):
        with conn.transaction():
            sm.transition_task(conn, t1.id, TaskStatus.RUNNING)  # PENDING → RUNNING is not allowed
    assert sm.get_task(conn, t1.id).status is TaskStatus.PENDING
    with pytest.raises(IllegalTransition):
        with conn.transaction():
            sm.transition_run(conn, run.id, RunStatus.PASSED)  # RUNNING → PASSED skips VERIFYING (I-3)
    assert sm.get_run(conn, run.id).status is RunStatus.RUNNING
