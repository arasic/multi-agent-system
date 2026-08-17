"""Step 6: git worktrees per attempt, real git_commit artifacts, input assembly, integration merge, context scoping.

Needs `git` on PATH (skipped otherwise) and Postgres for the end-to-end parts.
"""

import shutil
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest

from mas.artifacts import store
from mas.models.enums import AttemptStatus, RunStatus, TaskStatus
from mas.models.types import Attempt, Run, Task
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import scheduler
from mas.orchestrator import state_machine as sm
from mas.planner.dag import DagSpec
from mas.planner.validator import validate
from mas.verifier.stub import StubVerifier
from mas.workers.runtime import Worker, run_worker_thread, wait_all
from mas.workers.stub import StubAgent
from mas.workers.workspace import GitWorkspace, WorkspaceHandle, _git, git_available
from tests.conftest import CAPS, DB_URL, default_budgets, diamond

pytestmark = [pytest.mark.db, pytest.mark.skipif(not git_available(), reason="git not on PATH")]


@pytest.fixture
def gws(tmp_path: Path) -> GitWorkspace:
    return GitWorkspace(tmp_path / "repos", tmp_path / "worktrees")


def _exec_git(conn, dag: DagSpec, gws: GitWorkspace, *, workers=3, sleep=0.05, budgets=None, verifier=None, timeout=60):
    run = runs_mod.create_run_from_dag(conn, dag, budgets=budgets or default_budgets(), capabilities=set(CAPS))
    stop = threading.Event()
    agent = StubAgent({"sleep_s": sleep})
    ws = [
        Worker(f"w{i}", list(CAPS), agent, database_url=DB_URL, poll_s=0.05, run_id=run.id, workspace=gws) for i in range(workers)
    ]
    ts = [run_worker_thread(w, stop) for w in ws]
    try:
        final = scheduler.run_until_terminal(
            conn, run.id, verifier=verifier or StubVerifier(True), workspace=gws, tick_s=0.1, timeout_s=timeout
        )
    finally:
        stop.set()
        wait_all(ts, 10)
    return final, ws


def _artifacts(conn, run_id, key):
    tasks = {t.key: t.id for t in sm.tasks_for_run(conn, run_id)}
    return store.for_task(conn, tasks[key])


# ----------------------------------------------------------------------------- lifecycle (no DB needed beyond fixtures)


def test_repo_and_worktree_lifecycle(conn, gws):
    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
    t1 = next(t for t in sm.tasks_for_run(conn, run.id) if t.key == "T1")
    repo = gws.ensure_repo(run.id)
    assert (repo / "HEAD").exists()
    base = gws.base_sha(run.id)
    assert gws.ensure_repo(run.id) == repo and gws.base_sha(run.id) == base  # idempotent
    att = Attempt(id=t1.id, task_id=t1.id, attempt_number=1, status=AttemptStatus.RUNNING)  # any uuid works for the path
    h = gws.create(run, t1, att, inputs=[])
    assert h.path.exists() and h.start_sha == base and h.branch.endswith(f"/{t1.id}/1") and h.meta["task_key"] == "T1"
    (h.path / "docs").mkdir()
    (h.path / "docs" / "design.md").write_text("hello", encoding="utf-8")
    assert gws.publish(h, "nothing yet", since="start") is not None  # something was written → a commit
    sha = _git("rev-parse", "HEAD", cwd=h.path).stdout.strip()
    assert gws.show(run.id, f"{sha}:docs/design.md") == "hello"
    gws.cleanup(h)
    assert not h.path.exists()
    assert _git("rev-parse", "--verify", h.branch, cwd=repo, check=False).returncode == 0  # branch/commit survive
    # a second attempt of the same task starts clean from base: nothing inherited from attempt 1
    att2 = Attempt(id=t1.id, task_id=t1.id, attempt_number=2, status=AttemptStatus.RUNNING)
    h2 = gws.create(run, t1, att2, inputs=[])
    assert h2.start_sha == base and not (h2.path / "docs" / "design.md").exists()
    assert gws.publish(h2, "no changes") is None  # nothing written → no commit → no artifact
    gws.cleanup(h2)


def test_attempt_creation_never_runs_a_global_worktree_prune(conn, gws, monkeypatch):
    """`git worktree prune` is global: it deletes the admin entry of every worktree whose directory is missing —
    including one a sibling `git worktree add` is creating right now (git makes <repo>/worktrees/<name>/ first and
    writes `gitdir` after). That race cost one attempt at config D / N=16 in the offline matrix:
    "fatal: could not open 'worktrees/<task>-1/gitdir' for writing". Only run-terminal GC may prune."""
    import mas.workers.workspace as ws_mod

    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
    t1 = next(t for t in sm.tasks_for_run(conn, run.id) if t.key == "T1")
    att = Attempt(id=t1.id, task_id=t1.id, attempt_number=1, status=AttemptStatus.RUNNING)

    calls: list[tuple[str, ...]] = []
    real = ws_mod._git
    monkeypatch.setattr(ws_mod, "_git", lambda *a, **kw: (calls.append(a), real(*a, **kw))[1])

    handle = gws.create(run, t1, att, inputs=[])  # includes ensure_repo on a fresh run
    assert not any(a[:2] == ("worktree", "prune") for a in calls), "the per-attempt path must not prune globally"
    gws.cleanup(handle)
    assert not any(a[:2] == ("worktree", "prune") for a in calls)

    calls.clear()
    gws.gc_run(run.id)  # ...but the terminal-run GC still cleans up what dead workers registered
    assert any(a[:2] == ("worktree", "prune") for a in calls)


def test_concurrent_attempt_worktrees_never_administer_the_repo_at_the_same_time(conn, gws, monkeypatch):
    """The width-16 shape: many attempts of one run create worktrees in the same bare repo at once.

    `worktree add|remove|prune` and `branch -f` all enumerate <repo>/worktrees/ and read each entry's gitdir/commondir,
    which `worktree add` writes *after* creating the directory — so two of them running at once can read a sibling
    being born ("fatal: failed to read worktrees/<id>/commondir"). Asserting "no run failed" only samples the race;
    this asserts the property that removes it: the administrative commands never overlap in time."""
    import mas.workers.workspace as ws_mod

    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
    tasks = sm.tasks_for_run(conn, run.id)
    gws.ensure_repo(run.id)  # the repo exists; the race under test is worktree administration, not initialization
    admin = {("worktree", "add"), ("worktree", "remove"), ("worktree", "prune"), ("branch", "-f")}
    spans: list[tuple[str, float, float]] = []
    guard = threading.Lock()
    real = ws_mod._git

    def timed(*args, **kw):
        if args[:2] not in admin:
            return real(*args, **kw)
        t0 = time.monotonic()
        try:
            return real(*args, **kw)
        finally:
            with guard:
                spans.append((" ".join(args[:2]), t0, time.monotonic()))

    monkeypatch.setattr(ws_mod, "_git", timed)
    results: dict[str, object] = {}
    barrier = threading.Barrier(len(tasks))

    def make(task) -> None:
        att = Attempt(id=task.id, task_id=task.id, attempt_number=1, status=AttemptStatus.RUNNING)
        try:
            barrier.wait(timeout=30)
            results[task.key] = gws.create(run, task, att, inputs=[])
        except Exception as exc:  # noqa: BLE001 - the failure is the assertion
            results[task.key] = exc

    threads = [threading.Thread(target=make, args=(t,)) for t in tasks]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    failures = {k: v for k, v in results.items() if isinstance(v, Exception)}
    assert not failures, failures
    assert len(results) == len(tasks) and all(h.path.is_dir() for h in results.values())
    assert len(spans) >= 2 * len(tasks)  # each create did at least `branch -f` and `worktree add`
    ordered = sorted(spans, key=lambda s: s[1])
    overlaps = [(a, b) for a, b in zip(ordered, ordered[1:], strict=False) if b[1] < a[2]]
    assert not overlaps, f"git administration overlapped: {overlaps}"
    for h in results.values():
        gws.cleanup(h)


def test_repo_admin_lock_excludes_another_process_and_is_released_on_exit(gws):
    """Cross-process, because workers are processes (and containers): one holder blocks every other holder."""
    from mas.workers.workspace import ADMIN_LOCK_FILE, FILE_LOCKING, WorkspaceError, repo_admin_lock

    if FILE_LOCKING == "none":  # pragma: no cover - neither fcntl nor msvcrt
        pytest.skip("no file locking on this platform")
    run_id = uuid.uuid4()
    repo = gws.ensure_repo(run_id)
    holder = subprocess.Popen(  # noqa: S603 - fixed argv, test-only
        [
            sys.executable,
            "-c",
            "import sys, time\n"
            "from pathlib import Path\n"
            "from mas.workers.workspace import repo_admin_lock\n"
            "with repo_admin_lock(Path(sys.argv[1])):\n"
            "    print('held', flush=True)\n"
            "    time.sleep(float(sys.argv[2]))\n",
            str(repo),
            "2.0",
        ],
        stdout=subprocess.PIPE,
        text=True,
        cwd=str(Path(__file__).resolve().parents[1]),
    )
    try:
        assert holder.stdout is not None and holder.stdout.readline().strip() == "held"
        with pytest.raises(WorkspaceError, match="administration lock"):
            with repo_admin_lock(repo, timeout_s=0.3):
                pass
        with repo_admin_lock(repo, timeout_s=0.3, required=False) as may_administer:
            assert may_administer is False  # cleanup/GC skip instead of failing
        assert holder.wait(timeout=20) == 0
    finally:
        holder.kill()
        holder.wait(timeout=10)
    with repo_admin_lock(repo, timeout_s=5.0) as may_administer:  # released when the holder exited
        assert may_administer is True
    assert (repo / ADMIN_LOCK_FILE).exists()


def test_a_filesystem_without_locking_degrades_to_in_process_serialization(gws, monkeypatch, caplog):
    """Some bind mounts refuse advisory locks. Failing every attempt there would be worse than the race: the
    in-process mutex still serializes this process's threads, and the fallback is logged, not silent."""
    import mas.workers.workspace as ws_mod

    run_id = uuid.uuid4()
    repo = gws.ensure_repo(run_id)
    monkeypatch.setattr(ws_mod, "_try_lock", lambda fd: None)  # ENOLCK/EINVAL: locking unsupported here
    monkeypatch.setattr(ws_mod, "_unsupported_warned", False)
    with caplog.at_level("WARNING"), ws_mod.repo_admin_lock(repo, timeout_s=0.2) as may_administer:
        assert may_administer is True  # proceed, do not raise
    assert any("does not support file locking" in r.message for r in caplog.records)

    monkeypatch.setattr(ws_mod, "_try_lock", lambda fd: False)  # ...but a lock genuinely held elsewhere still waits out
    with pytest.raises(ws_mod.WorkspaceError, match="administration lock"):
        with ws_mod.repo_admin_lock(repo, timeout_s=0.2):
            pass


def test_dead_worker_worktree_is_pruned_and_branch_reset(conn, gws):
    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(), capabilities=set(CAPS))
    t1 = next(t for t in sm.tasks_for_run(conn, run.id) if t.key == "T1")
    att = Attempt(id=t1.id, task_id=t1.id, attempt_number=1, status=AttemptStatus.RUNNING)
    h = gws.create(run, t1, att, inputs=[])
    (h.path / "half.txt").write_text("half-done", encoding="utf-8")
    shutil.rmtree(h.path)  # the worker died and its worktree directory vanished (or is stale)
    h_again = gws.create(
        run, t1, att, inputs=[]
    )  # same attempt number re-created (e.g. after ABANDONED → new attempt uses n+1; here we test robustness)
    assert h_again.path.exists() and not (h_again.path / "half.txt").exists()
    gws.cleanup(h_again)


# ----------------------------------------------------------------------------- end-to-end in git mode


def test_diamond_end_to_end_with_real_commits(conn, gws):
    final, _ = _exec_git(conn, diamond(), gws)
    assert final.status is RunStatus.PASSED
    rid = final.id
    # T1: document artifact resolved to <sha>:docs/design.md plus the commit that contains it
    a1 = _artifacts(conn, rid, "T1")
    doc = next(a for a in a1 if a.type == "document")
    c1 = next(a for a in a1 if a.type == "git_commit")
    assert doc.ref.startswith(c1.ref) and doc.ref.endswith(":docs/design.md")
    assert "stub document for T1" in gws.show(rid, doc.ref)
    # T2..T4: input assembly — each worktree contained T1's commit, and added its own file
    for k in ("T2", "T3", "T4"):
        c = next(a for a in _artifacts(conn, rid, k) if a.type == "git_commit")
        files = gws.files_at(rid, c.ref)
        assert "docs/design.md" in files and f"src/{k}.txt" in files
        assert gws.is_ancestor(rid, c1.ref, c.ref)
        assert c.meta["merged"] == [c1.ref]
    # T5 integration: the assembled merge is the output and contains everything; accepted on PASS
    a5 = _artifacts(conn, rid, "T5")
    integ = next(a for a in a5 if a.type == "git_commit")
    assert integ.status.value == "accepted"
    files = gws.files_at(rid, integ.ref)
    assert {"docs/design.md", "src/T2.txt", "src/T3.txt", "src/T4.txt"} <= set(files)
    for k in ("T2", "T3", "T4"):
        ck = next(a for a in _artifacts(conn, rid, k) if a.type == "git_commit")
        assert gws.is_ancestor(rid, ck.ref, integ.ref)
    # run/<run>/integration promoted to the accepted commit
    ref = _git("rev-parse", f"refs/heads/run/{rid}/integration", cwd=gws.repo_path(rid)).stdout.strip()
    assert ref == integ.ref
    # worktrees cleaned up; bare repo keeps every attempt branch
    assert not any((gws.worktree_root / str(rid)).glob("*")) or not (gws.worktree_root / str(rid)).exists()
    branches = _git("for-each-ref", "--format=%(refname:short)", f"refs/heads/run/{rid}/", cwd=gws.repo_path(rid)).stdout.split()
    assert len([b for b in branches if b.count("/") == 3]) == 5  # one attempt branch per task


def test_conflicting_inputs_surface_as_failed_integration_not_silent_merge(conn, gws):
    """Two upstream tasks write the same file differently → the integration worktree gets a real merge conflict.
    The stub cannot resolve it → attempts fail with the conflicting paths → run FAILS. Nothing was averaged away."""
    d = diamond({"T2": {"files": {"shared.txt": "from T2\n"}}, "T3": {"files": {"shared.txt": "from T3\n"}}})
    final, _ = _exec_git(conn, d, gws, budgets=default_budgets(max_attempts_per_task=2))
    assert final.status is RunStatus.FAILED
    assert "T5 failed" in final.verdict
    atts = [
        a
        for a in sm.attempts_for_run(conn, final.id)
        if a.task_id == next(t.id for t in sm.tasks_for_run(conn, final.id) if t.key == "T5")
    ]
    assert len(atts) == 2 and all(a.status is AttemptStatus.FAILED for a in atts)
    assert all("unresolved merge conflicts" in (a.failure_reason or "") and "shared.txt" in a.failure_reason for a in atts)
    # no git_commit artifact was published for the conflicted attempts (nothing valid to publish)
    assert not [a for a in _artifacts(conn, final.id, "T5") if a.type == "git_commit"]


def test_context_spec_artifacts_from_is_enforced(conn, gws):
    """T3 depends on T1 and T2 but is scoped to T2's outputs only: it sees (and merges) only T2."""
    d = DagSpec.from_dict(
        {
            "tasks": [
                {
                    "id": "T1",
                    "capability": "architecture",
                    "goal": "",
                    "depends_on": [],
                    "output_contract": {"artifacts": ["document:design.md"]},
                },
                {
                    "id": "T2",
                    "capability": "implementation",
                    "goal": "",
                    "depends_on": ["T1"],
                    "output_contract": {"artifacts": ["git_commit"]},
                },
                {
                    "id": "T3",
                    "capability": "implementation",
                    "goal": "",
                    "depends_on": ["T1", "T2"],
                    "output_contract": {"artifacts": ["git_commit"]},
                    "context_spec": {"artifacts_from": ["T2"]},
                },
                {
                    "id": "T4",
                    "capability": "integration",
                    "goal": "",
                    "depends_on": ["T3"],
                    "output_contract": {"artifacts": ["git_commit"]},
                },
            ]
        }
    )
    final, _ = _exec_git(conn, d, gws)
    assert final.status is RunStatus.PASSED
    c2 = next(a for a in _artifacts(conn, final.id, "T2") if a.type == "git_commit")
    c3 = next(a for a in _artifacts(conn, final.id, "T3") if a.type == "git_commit")
    assert c3.meta["merged"] == [c2.ref]  # only T2 was assembled, not T1 directly (T1 arrives through T2's history)
    doc3 = gws.show(final.id, f"{c3.ref}:src/T3.txt")
    assert "document" not in doc3  # inputs note lists only T2's commit
    assert "git_commit" in doc3


def test_validator_rule10_artifacts_from_must_be_dependencies():
    d = diamond()
    d.by_id()["T2"].context_spec = {"artifacts_from": ["T3"]}  # sibling, not a dependency
    r = validate(d)
    assert any(e.rule == "10" and e.task_id == "T2" and "not a dependency" in e.message for e in r.errors)
    d.by_id()["T2"].context_spec = {"artifacts_from": ["T9"]}
    assert any(e.rule == "10" and "unknown task" in e.message for e in validate(d).errors)
    d.by_id()["T2"].context_spec = {"artifacts_from": ["T1"]}
    assert validate(d).ok
    d.by_id()["T5"].context_spec = {"artifacts_from": ["T1"]}  # transitive dependency is fine
    assert validate(d).ok


def test_task_output_contract_unmet_when_agent_writes_nothing(conn, gws):
    """git mode: no file written → no commit → no git_commit artifact → contract unmet → retries → run FAILED."""
    d = diamond({"T2": {"artifacts": []}})  # T2 requires git_commit but the stub writes nothing
    final, _ = _exec_git(conn, d, gws, budgets=default_budgets(max_attempts_per_task=2))
    assert final.status is RunStatus.FAILED
    t2 = next(t for t in sm.tasks_for_run(conn, final.id) if t.key == "T2")
    assert t2.status is TaskStatus.FAILED
    reasons = [a.failure_reason for a in sm.attempts_for_run(conn, final.id) if a.task_id == t2.id]
    assert all("output contract unmet: git_commit" in (r or "") for r in reasons)


def test_compose_hardening_declared():
    """Sanity: worker/orchestrator services are non-root-ready, read-only, no egress, acceptance read-only."""
    text = Path("docker-compose.yml").read_text(encoding="utf-8")
    assert "internal: true" in text
    assert text.count("read_only: true") >= 2
    assert text.count("cap_drop: [ALL]") >= 2
    assert "./acceptance:/app/acceptance:ro" in text
    docker = Path("Dockerfile").read_text(encoding="utf-8")
    assert "USER mas" in docker and "useradd" in docker


def test_handle_dataclass_defaults():
    h = WorkspaceHandle(path=Path("."), repo=Path("."), branch="b", base_sha="x", start_sha="x")
    assert h.conflicts == [] and h.merged == []


def _unused(run: Run, task: Task):  # keep imports honest for type checkers
    return run, task


# ----------------------------------------------------------------------------- stabilization (M1 follow-up)


def test_two_root_tasks_initialize_the_same_repo_concurrently(conn, gws):
    """Race: two workers create worktrees for two independent root tasks of a brand-new run at the same time.
    Both must succeed, one repo must exist, both must start from the same base."""
    import concurrent.futures as cf

    d = DagSpec.from_dict(
        {
            "tasks": [
                {
                    "id": "A",
                    "capability": "implementation",
                    "goal": "",
                    "depends_on": [],
                    "output_contract": {"artifacts": ["git_commit"]},
                },
                {
                    "id": "B",
                    "capability": "implementation",
                    "goal": "",
                    "depends_on": [],
                    "output_contract": {"artifacts": ["git_commit"]},
                },
                {
                    "id": "I",
                    "capability": "integration",
                    "goal": "",
                    "depends_on": ["A", "B"],
                    "output_contract": {"artifacts": ["git_commit"]},
                },
            ]
        }
    )
    for _ in range(3):  # repeat with fresh runs: the race window is small
        run = runs_mod.create_run_from_dag(conn, d, budgets=default_budgets(), capabilities=set(CAPS))
        tasks = {t.key: t for t in sm.tasks_for_run(conn, run.id)}

        def make(key, run=run, tasks=tasks):
            t = tasks[key]
            att = Attempt(id=t.id, task_id=t.id, attempt_number=1, status=AttemptStatus.RUNNING)
            h = gws.create(run, t, att, inputs=[])
            (h.path / f"{key}.txt").write_text(key, encoding="utf-8")
            sha = gws.publish(h, key)
            gws.cleanup(h)
            return h.base_sha, sha

        with cf.ThreadPoolExecutor(2) as ex:
            (base_a, sha_a), (base_b, sha_b) = list(ex.map(make, ["A", "B"]))
        assert base_a == base_b == gws.base_sha(run.id)
        assert sha_a and sha_b and sha_a != sha_b
        assert not list(gws.repo_root.glob(f"{run.id}.init-*"))  # no leftover temp init dirs


def test_unsafe_task_ids_are_rejected():
    for bad in ["../x", "a/b", "a b", "", ".hidden", "x" * 65, "T1;rm", "a..b"]:
        d = diamond()
        d.tasks[1].id = bad
        d.tasks[4].depends_on = [x if x != "T2" else bad for x in d.tasks[4].depends_on]
        r = validate(d)
        assert any(e.rule == "id" for e in r.errors), bad
    d = diamond()
    d.tasks[1].id = "task_2.v1-final"
    d.tasks[4].depends_on = [x if x != "T2" else "task_2.v1-final" for x in d.tasks[4].depends_on]
    assert not any(e.rule == "id" for e in validate(d).errors)


def test_slow_publish_is_not_reaped(conn, gws):
    """Heartbeat must run through settlement: a workspace whose publish() sleeps longer than the lease
    must still complete SUCCESS (no ABANDONED, no stale report)."""
    import time as _time

    class SlowPublish(GitWorkspace):
        def publish(self, handle, message, *, since="start"):
            _time.sleep(2.6)  # > 2 leases of 1 s
            return super().publish(handle, message, since=since)

    slow = SlowPublish(gws.repo_root, gws.worktree_root)
    d = DagSpec.from_dict(
        {
            "tasks": [
                {
                    "id": "A",
                    "capability": "implementation",
                    "goal": "",
                    "depends_on": [],
                    "output_contract": {"artifacts": ["git_commit"]},
                },
                {
                    "id": "I",
                    "capability": "integration",
                    "goal": "",
                    "depends_on": ["A"],
                    "output_contract": {"artifacts": ["git_commit"]},
                },
            ]
        }
    )
    run = runs_mod.create_run_from_dag(conn, d, budgets=default_budgets(lease_s=1), capabilities=set(CAPS))
    stop = threading.Event()
    w = Worker(
        "w-slow", list(CAPS), StubAgent({"sleep_s": 0.05}), database_url=DB_URL, poll_s=0.05, run_id=run.id, workspace=slow
    )
    t = run_worker_thread(w, stop)
    try:
        final = scheduler.run_until_terminal(conn, run.id, verifier=StubVerifier(True), workspace=slow, tick_s=0.1, timeout_s=60)
    finally:
        stop.set()
        wait_all([t], 10)
    assert final.status is RunStatus.PASSED
    atts = sm.attempts_for_run(conn, run.id)
    assert [a.status for a in atts] == [AttemptStatus.SUCCESS, AttemptStatus.SUCCESS]
    assert w.stats.stale == 0 and w.stats.completed == 2


def test_report_is_atomic_stale_publishes_nothing(conn):
    """If the attempt was reaped before the report lands, the report publishes NO artifacts and settles nothing."""
    from mas.orchestrator import leases

    run = runs_mod.create_run_from_dag(conn, diamond(), budgets=default_budgets(lease_s=1), capabilities=set(CAPS))
    scheduler.tick(conn, run.id)
    c = leases.claim_task(conn, worker_id="w", capabilities=list(CAPS), lease_s=1)
    assert c is not None
    with conn.transaction():
        conn.execute("UPDATE attempts SET lease_until = now() - interval '5 seconds' WHERE id = %s", (c.attempt.id,))
    assert leases.reap_expired(conn, run.id) == [(c.attempt.id, AttemptStatus.ABANDONED)]
    with pytest.raises(leases.StaleAttempt):
        leases.report(
            conn,
            c.attempt.id,
            success=True,
            artifacts=[leases.ArtifactSpec(type="document", ref="stub:late", meta={"name": "design.md"})],
        )
    assert store.for_attempt(conn, c.attempt.id) == []  # nothing leaked from the aborted transaction
    assert sm.get_attempt(conn, c.attempt.id).status is AttemptStatus.ABANDONED
