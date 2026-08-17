"""CLI smoke tests: the commands a human runs must work end-to-end (in-process, stub workers, no API key)."""

import pytest

from mas.cli import main

pytestmark = pytest.mark.db

DAG = "benchmarks/url_shortener/dag.json"


def test_migrate_and_run_and_status_and_replay(conn, capsys):
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
