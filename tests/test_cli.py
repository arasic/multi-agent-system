"""CLI smoke tests: the commands a human runs must work end-to-end (in-process, stub workers, no API key)."""

import json

import pytest

from mas.cli import main
from mas.planner.dag import plan_digest

pytestmark = pytest.mark.db

DAG = "benchmarks/url_shortener/dag.json"


def test_migrate_and_run_and_status_and_replay_and_export(conn, capsys, tmp_path):
    assert main(["migrate"]) == 0
    rc = main(["run", "--dag", DAG, "--workers", "3", "--stub-sleep", "0.05", "--lease-s", "2", "--stub-verifier"])
    out = capsys.readouterr().out
    assert rc == 0, out
    assert "PASSED  verdict=PASS" in out
    assert "max_concurrent=" in out
    run_id = out.split("run ", 1)[1].split()[0]
    assert main(["status", run_id]) == 0
    assert "PASSED" in capsys.readouterr().out
    assert main(["replay", run_id]) == 0
    replay = capsys.readouterr().out
    assert "run.created" in replay and "run.passed" in replay and "attempt.leased" in replay
    assert main(["status", run_id, "--json"]) == 0
    assert '"status": "PASSED"' in capsys.readouterr().out
    assert main(["result", run_id]) == 0
    record = json.loads(capsys.readouterr().out)
    assert record["status"] == "PASSED" and len(record["integration_sha"]) == 40
    output = tmp_path / "verified"
    assert main(["result", run_id, "--output", str(output)]) == 0
    assert (output / ".git").is_dir() and (tmp_path / "verified.mas-result.json").is_file()
    assert main(["result", run_id, "--output", str(output)]) == 2  # never overwrites


def test_run_with_chaos_kill_recovers(conn, capsys):
    rc = main(
        [
            "run",
            "--dag",
            DAG,
            "--workers",
            "3",
            "--stub-sleep",
            "0.8",
            "--lease-s",
            "1",
            "--chaos-kill-after",
            "0.3",
            "--stub-verifier",
        ]
    )
    out = capsys.readouterr().out
    assert rc == 0, out
    assert "[chaos] killed worker-" in out and "while on T" in out  # a busy worker was killed
    assert "abandoned=1" in out
    assert "died=1" in out


def test_run_verifier_fail_exit_code(conn, capsys):
    rc = main(["run", "--dag", DAG, "--workers", "2", "--stub-sleep", "0.02", "--verifier-fail"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "FAILED  verdict=FAIL:verification failed" in out and "reason=UNRECOVERABLE_FAILURE" in out


def test_run_bounded_repair_demo_fail_then_pass(conn, capsys):
    """13-lite offline demo: stub verifier FAILs once, the fake planner double proposes an amendment (R1 + R1_integrate on
    top of the last integration), stub workers run it, the verifier passes; exactly one replan used."""
    rc = main(
        [
            "run",
            "--dag",
            DAG,
            "--workers",
            "2",
            "--stub-sleep",
            "0.02",
            "--verifier-fail-times",
            "1",
            "--planner",
            "fake",
            "--max-replans",
            "1",
            "--workspace",
            "none",
        ]
    )
    out = capsys.readouterr().out
    assert rc == 0, out
    assert "PASSED  verdict=PASS" in out and "replans=1" in out
    assert "R1_integrate" in out and "R1 " in out


def test_run_bounded_repair_no_progress_verdict(conn, capsys):
    rc = main(
        [
            "run",
            "--dag",
            DAG,
            "--workers",
            "2",
            "--stub-sleep",
            "0.02",
            "--verifier-fail",
            "--planner",
            "fake",
            "--max-replans",
            "1",
            "--workspace",
            "none",
        ]
    )
    out = capsys.readouterr().out
    assert rc == 1
    assert "reason=NO_PROGRESS" in out and "replans=1" in out


def test_submit_creates_run_and_exits(conn, capsys):
    rc = main(["submit", "--dag", DAG])
    out = capsys.readouterr().out
    assert rc == 0 and "submitted run" in out and "status=RUNNING" in out
    row = conn.execute("SELECT status FROM runs").fetchone()
    assert row["status"] == "RUNNING"  # waits for the orchestrator/worker services


def test_configs_a_and_c_change_the_runtime_shape_not_only_the_label(conn, capsys):
    assert main(["run", "--dag", DAG, "--config", "A", "--stub-verifier", "--stub-sleep", "0.01"]) == 0
    out = capsys.readouterr().out
    assert "config=A, 2 tasks" in out and "max_concurrency=1" in out
    row = conn.execute("SELECT config, max_concurrency FROM runs ORDER BY created_at DESC LIMIT 1").fetchone()
    assert row == {"config": "A", "max_concurrency": 1}

    assert main(["submit", "--dag", DAG, "--config", "C", "--max-concurrency", "9"]) == 0
    row = conn.execute("SELECT config, max_concurrency FROM runs ORDER BY created_at DESC LIMIT 1").fetchone()
    assert row == {"config": "C", "max_concurrency": 1}


def test_plan_exports_one_validated_dag_and_never_executes_it(conn, capsys, tmp_path):
    """ADR-009: `mas plan` is a real, metered planning round whose run is ended as CANCELLED — the exported DAG is
    exactly what the validator installed, and configs C and D can then replay that same plan."""
    out_file = tmp_path / "plan.json"
    rc = main(
        [
            "plan",
            "--goal",
            "Implement two independent adapters with a passing pytest suite.",
            "--benchmark",
            "adapters_2",
            "--planner",
            "fake",
            "--output",
            str(out_file),
            "--max-concurrency",
            "2",
            "--json",
        ]
    )
    record = json.loads(capsys.readouterr().out)
    assert rc == 0 and record["planned"] and not record["parked"]
    assert record["status"] == "ABORTED" and record["verdict_reason"] == "CANCELLED"
    assert record["model_calls"] >= 1  # the planner really ran and was metered

    row = conn.execute("SELECT status, verdict_reason FROM runs WHERE id = %s", (record["run_id"],)).fetchone()
    assert row == {"status": "ABORTED", "verdict_reason": "CANCELLED"}  # never left open, never counted as evidence
    attempts = conn.execute(
        "SELECT count(*) AS n FROM attempts a JOIN tasks t ON t.id = a.task_id WHERE t.run_id = %s", (record["run_id"],)
    ).fetchone()
    assert attempts["n"] == 0  # planned, not executed

    exported = json.loads(out_file.read_text(encoding="utf-8"))
    assert plan_digest(exported) == record["plan_sha256"] and len(exported["tasks"]) == record["tasks"]
    stored = conn.execute("SELECT meta FROM artifacts WHERE run_id = %s AND type = 'plan'", (record["run_id"],)).fetchone()
    assert stored["meta"]["dag"] == exported  # the file is the validated plan on record, not the raw proposal

    # ...and that exported plan is executable as-is: the same decomposition, run sequentially (config C)
    assert main(["run", "--dag", str(out_file), "--config", "C", "--stub-verifier", "--stub-sleep", "0.01"]) == 0
    assert "max_concurrency=1" in capsys.readouterr().out


def test_single_agent_verifier_repair_remains_single_agent(conn, capsys):
    rc = main(
        [
            "run",
            "--dag",
            DAG,
            "--config",
            "A",
            "--verifier-fail-times",
            "1",
            "--max-replans",
            "1",
            "--stub-sleep",
            "0.01",
        ]
    )
    out = capsys.readouterr().out
    assert rc == 0, out
    assert "SOLVE_R1" in out and "INTEGRATE_R1" in out and "replans=1" in out
