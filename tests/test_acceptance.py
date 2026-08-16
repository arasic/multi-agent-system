"""Step 7A: exact-commit, fail-closed acceptance verification in a real sandbox."""

from __future__ import annotations

import os
import shutil
import subprocess
import threading
from pathlib import Path
from uuid import uuid4

import pytest

from mas.artifacts import store
from mas.models.enums import ArtifactStatus, RunStatus
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import scheduler
from mas.planner.dag import DagSpec
from mas.verifier.acceptance import AcceptanceVerifier, SandboxLimits
from mas.verifier.base import VerificationRequest, VerificationStatus
from mas.workers.runtime import Worker, run_worker_thread, wait_all
from mas.workers.stub import StubAgent
from mas.workers.workspace import GitWorkspace
from tests.conftest import CAPS, DB_URL, default_budgets

ROOT = Path(__file__).resolve().parents[1]
APPS = ROOT / "tests" / "fixtures" / "apps"
SPECIAL_SUITES = ROOT / "tests" / "fixtures" / "acceptance"


def _run(*args: str, cwd: Path) -> str:
    p = subprocess.run(args, cwd=cwd, capture_output=True, text=True, check=False)
    assert p.returncode == 0, p.stderr
    return p.stdout.strip()


def _fixture_commit(tmp_path: Path, fixture: str) -> tuple[Path, str]:
    source = tmp_path / f"source-{fixture}"
    shutil.copytree(APPS / fixture, source)
    _run("git", "init", "-q", "--initial-branch=main", cwd=source)
    _run("git", "config", "user.name", "fixture", cwd=source)
    _run("git", "config", "user.email", "fixture@example.invalid", cwd=source)
    _run("git", "add", "-A", cwd=source)
    _run("git", "commit", "-q", "-m", fixture, cwd=source)
    sha = _run("git", "rev-parse", "HEAD", cwd=source)
    bare = tmp_path / f"{fixture}.git"
    _run("git", "clone", "-q", "--bare", str(source), str(bare), cwd=tmp_path)
    return bare, sha


def _request(repository: Path, sha: str, benchmark: str = "url_shortener") -> VerificationRequest:
    return VerificationRequest(run_id=uuid4(), benchmark=benchmark, repository=repository, commit_sha=sha)


@pytest.mark.docker
def test_known_good_fixture_passes_with_auditable_evidence(tmp_path, verifier_image):
    repo, sha = _fixture_commit(tmp_path, "known_good")
    result = AcceptanceVerifier(ROOT / "acceptance", image=verifier_image).verify(_request(repo, sha))
    assert result.status is VerificationStatus.PASS, result.report
    assert [c.id for c in result.checks] == [
        "service_starts",
        "shorten_returns_201",
        "resolve_redirects",
        "stats_available",
        "restart_persists",
    ]
    assert result.evidence["integration_sha"] == sha
    assert len(result.evidence["suite_sha256"]) == 64
    assert result.evidence["sandbox_image"].startswith(("sha256:", verifier_image + "@"))


@pytest.mark.docker
def test_failing_endpoint_fixture_fails(tmp_path, verifier_image):
    repo, sha = _fixture_commit(tmp_path, "failing_endpoint")
    result = AcceptanceVerifier(ROOT / "acceptance", image=verifier_image).verify(_request(repo, sha))
    assert result.status is VerificationStatus.FAIL
    assert next(c for c in result.checks if c.id == "shorten_returns_201").status.value == "FAIL"


@pytest.mark.docker
def test_hanging_suite_is_killed_by_host_timeout(tmp_path, verifier_image):
    repo, sha = _fixture_commit(tmp_path, "known_good")
    verifier = AcceptanceVerifier(SPECIAL_SUITES, image=verifier_image, limits=SandboxLimits(timeout_s=3))
    result = verifier.verify(_request(repo, sha, "hangs"))
    assert result.status is VerificationStatus.TIMEOUT
    assert "hard timeout" in (result.reason or "")


@pytest.mark.docker
def test_application_cannot_use_external_network(tmp_path, verifier_image):
    repo, sha = _fixture_commit(tmp_path, "tries_network")
    result = AcceptanceVerifier(ROOT / "acceptance", image=verifier_image).verify(_request(repo, sha))
    assert result.status is VerificationStatus.FAIL
    starts = next(c for c in result.checks if c.id == "service_starts")
    assert starts.status.value == "FAIL" and "exited" in starts.detail


@pytest.mark.docker
def test_missing_check_invalidates_entire_report(tmp_path, verifier_image):
    repo, sha = _fixture_commit(tmp_path, "known_good")
    verifier = AcceptanceVerifier(SPECIAL_SUITES, image=verifier_image, limits=SandboxLimits(timeout_s=10))
    result = verifier.verify(_request(repo, sha, "missing_check"))
    assert result.status is VerificationStatus.INVALID
    assert result.evidence["expected_checks"] == ["reported", "missing"]
    assert result.evidence["reported_checks"] == ["reported"]


def test_missing_suite_commit_and_runner_all_fail_closed(tmp_path):
    repo, sha = _fixture_commit(tmp_path, "known_good")
    missing_suite = AcceptanceVerifier(SPECIAL_SUITES, image="image-does-not-matter").verify(_request(repo, sha, "unknown"))
    assert missing_suite.status is VerificationStatus.INVALID
    missing_commit = AcceptanceVerifier(ROOT / "acceptance", image="image-does-not-matter").verify(
        VerificationRequest(uuid4(), "url_shortener", repo, None)
    )
    assert missing_commit.status is VerificationStatus.INVALID
    missing_runner = AcceptanceVerifier(ROOT / "acceptance", image="definitely-not-a-real-image:missing").verify(
        _request(repo, sha)
    )
    assert missing_runner.status is VerificationStatus.ERROR


@pytest.mark.db
@pytest.mark.docker
def test_worker_commit_to_external_verdict_end_to_end(conn, tmp_path, verifier_image):
    app = (APPS / "known_good" / "app.py").read_text(encoding="utf-8")
    dag = DagSpec.from_dict(
        {
            "goal": "fixture URL shortener",
            "benchmark": "url_shortener",
            "tasks": [
                {
                    "id": "build",
                    "capability": "implementation",
                    "goal": "write application",
                    "depends_on": [],
                    "output_contract": {"artifacts": ["git_commit"]},
                    "meta": {"stub": {"files": {"app.py": app}}},
                },
                {
                    "id": "integrate",
                    "capability": "integration",
                    "goal": "integrate application",
                    "depends_on": ["build"],
                    "output_contract": {"artifacts": ["git_commit"]},
                },
            ],
        }
    )
    run = runs_mod.create_run_from_dag(conn, dag, budgets=default_budgets(), capabilities=set(CAPS))
    workspace = GitWorkspace(tmp_path / "repos", tmp_path / "worktrees")
    stop = threading.Event()
    worker = Worker(
        "acceptance-fixture-worker",
        list(CAPS),
        StubAgent({"sleep_s": 0.01}),
        database_url=DB_URL,
        poll_s=0.02,
        run_id=run.id,
        workspace=workspace,
    )
    thread = run_worker_thread(worker, stop)
    verifier = AcceptanceVerifier(ROOT / "acceptance", image=verifier_image)
    try:
        final = scheduler.run_until_terminal(conn, run.id, verifier=verifier, workspace=workspace, tick_s=0.05, timeout_s=45)
    finally:
        stop.set()
        wait_all([thread], 10)

    assert final.status is RunStatus.PASSED
    verification = conn.execute(
        "SELECT status, meta FROM artifacts WHERE run_id=%s AND type='verification'", (run.id,)
    ).fetchone()
    assert verification["status"] == ArtifactStatus.CANDIDATE.value
    assert verification["meta"]["status"] == "PASS"
    assert verification["meta"]["report"]["integration_sha"]
    accepted = store.accepted_for_run(conn, run.id)
    assert len(accepted) == 1 and accepted[0].type == "git_commit"


@pytest.mark.docker
def test_output_flood_is_capped_killed_and_never_reaches_host_disk(tmp_path, verifier_image):
    """P1 from review: capture is bounded while capturing, not after. A 400 MB flood must produce INVALID quickly,
    leave no container, and never create a capture file on the host (pipes + in-memory cap only)."""
    import glob
    import tempfile
    import time

    repo, sha = _fixture_commit(tmp_path, "known_good")
    before = set(glob.glob(os.path.join(tempfile.gettempdir(), "mas-verify-*")))
    t0 = time.monotonic()
    result = AcceptanceVerifier(SPECIAL_SUITES, image=verifier_image, limits=SandboxLimits(timeout_s=20)).verify(
        _request(repo, sha, "floods")
    )
    elapsed = time.monotonic() - t0
    assert result.status is VerificationStatus.INVALID
    assert "output exceeded limit" in (result.reason or "")
    assert result.evidence["stdout_bytes"] > result.evidence["output_cap"] == 256 * 1024
    assert elapsed < 15, elapsed  # killed on overflow, well before the 20 s timeout
    assert set(glob.glob(os.path.join(tempfile.gettempdir(), "mas-verify-*"))) - before == set()
    leftover = subprocess.run(
        ["docker", "ps", "-a", "--filter", "name=mas-verify-", "-q"], capture_output=True, text=True, check=False
    ).stdout.split()
    assert leftover == []


def test_expected_suite_hash_is_enforced_before_any_sandbox_work(tmp_path):
    """ADR-007 pinning: a request carrying an approved suite hash that does not match the suite on disk is INVALID
    before the runner is even inspected (no Docker needed)."""
    repo, sha = _fixture_commit(tmp_path, "known_good")
    v = AcceptanceVerifier(ROOT / "acceptance", image="image-does-not-matter")
    good = v.suite_digest("url_shortener")
    assert len(good) == 64
    bad = VerificationRequest(uuid4(), "url_shortener", repo, sha, expected_suite_sha256="0" * 64)
    result = v.verify(bad)
    assert result.status is VerificationStatus.INVALID
    assert "approved contract" in (result.reason or "")
    assert result.evidence["suite_sha256"] == good
    # with the right hash we get past pinning; the missing image is then the (ERROR) reason, proving order
    ok = VerificationRequest(uuid4(), "url_shortener", repo, sha, expected_suite_sha256=good.upper())
    assert v.verify(ok).status is VerificationStatus.ERROR
