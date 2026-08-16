"""Step-6 stability gate (docs/roadmap.md). Not a unit test — a focused stress run with hard pass criteria.

Scenarios (defaults; override with flags):
  --diamonds 100    git-workspace diamond runs, lease_s=1, 3 in-process workers each
  --chaos 50        git-workspace diamond runs where a busy worker is killed mid-attempt (deliberate death)
  --parallel 20     runs submitted simultaneously to a shared pool of unpinned workers (service-like)
Pass criteria: zero deadlocks (any log line mentioning "deadlock"), zero stale reports outside deliberate deaths,
exactly one ABANDONED attempt per deliberate death, zero leaked worktrees, every run PASSED.

Usage:  .venv/Scripts/python scripts/stress_step6.py [--diamonds N] [--chaos N] [--parallel N] [--db URL]
Uses its own throwaway database (mas_stress_<uuid>) and temp repo/worktree roots. Exit code 0 = gate passed.
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from mas.db import connect, migrate
from mas.models.enums import AttemptStatus, RunStatus
from mas.orchestrator import leases, scheduler
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import state_machine as sm
from mas.planner.dag import DagSpec
from mas.verifier.stub import StubVerifier
from mas.workers.runtime import Worker, run_worker_thread, wait_all
from mas.workers.stub import StubAgent
from mas.workers.workspace import GitWorkspace

CAPS = ("architecture", "implementation", "testing", "integration", "solve")


def _t(id: str, cap: str, deps: list[str], arts: list[str]) -> dict:
    return {"id": id, "capability": cap, "goal": f"do {id}", "depends_on": deps, "output_contract": {"artifacts": arts}}


def diamond() -> DagSpec:
    return DagSpec.from_dict(
        {
            "goal": "diamond",
            "tasks": [
                _t("T1", "architecture", [], ["document:design.md"]),
                _t("T2", "implementation", ["T1"], ["git_commit"]),
                _t("T3", "implementation", ["T1"], ["git_commit"]),
                _t("T4", "implementation", ["T1"], ["git_commit"]),
                _t("T5", "integration", ["T2", "T3", "T4"], ["git_commit"]),
            ],
        }
    )


class Counter(logging.Handler):
    def __init__(self):
        super().__init__()
        self.deadlocks = 0
        self.errors: list[str] = []

    def emit(self, record):
        text = record.getMessage() + (str(record.exc_info[1]) if record.exc_info and record.exc_info[1] else "")
        if "deadlock" in text.lower():
            self.deadlocks += 1
        if record.levelno >= logging.ERROR:
            self.errors.append(text[:160])


@dataclass
class Tally:
    runs: int = 0
    passed: int = 0
    stale: int = 0
    died: int = 0
    abandoned: int = 0
    leaked_worktrees: int = 0
    failures: list[str] = field(default_factory=list)
    seconds: float = 0.0


def budgets(lease_s=1):
    from mas.models.types import Budgets

    return Budgets(max_concurrency=4, max_attempts_per_task=3, max_wallclock_s=120, max_attempt_runtime_s=60, lease_s=lease_s)


def one_run(db_url, gws, *, chaos: bool, tally: Tally, sleep=0.05):
    conn = connect(db_url)
    try:
        run = runs_mod.create_run_from_dag(conn, diamond(), budgets=budgets(), capabilities=set(CAPS))
        stop = threading.Event()
        agent = StubAgent({"sleep_s": sleep if not chaos else 0.6})
        ws = [
            Worker(f"w{i}", list(CAPS), agent, database_url=db_url, poll_s=0.02, run_id=run.id, workspace=gws) for i in range(3)
        ]
        threads = [run_worker_thread(w, stop) for w in ws]
        if chaos:

            def _chaos():
                deadline = time.monotonic() + 10
                victim = None
                while victim is None and time.monotonic() < deadline:
                    victim = next((w for w in ws if w.busy), None)
                    time.sleep(0.01)
                if victim is not None:
                    victim.die()

            threading.Thread(target=_chaos, daemon=True).start()
        t0 = time.monotonic()
        try:
            final = scheduler.run_until_terminal(
                conn, run.id, verifier=StubVerifier(True), workspace=gws, tick_s=0.05, timeout_s=150
            )
        finally:
            stop.set()
            wait_all(threads, 10)
        tally.seconds += time.monotonic() - t0
        tally.runs += 1
        if final.status is RunStatus.PASSED:
            tally.passed += 1
        else:
            tally.failures.append(f"{run.id}: {final.status.value} {final.verdict}")
        tally.stale += sum(w.stats.stale for w in ws)
        died = sum(1 for w in ws if w.stats.died)
        tally.died += died
        atts = sm.attempts_for_run(conn, run.id)
        ab = sum(1 for a in atts if a.status is AttemptStatus.ABANDONED)
        tally.abandoned += ab
        if chaos and died == 1 and ab != 1:
            tally.failures.append(f"{run.id}: died={died} abandoned={ab}")
        if not chaos and ab:
            tally.failures.append(f"{run.id}: unexpected abandoned={ab}")
        d = gws.worktree_root / str(run.id)
        if chaos:
            # the dead worker's worktree is intentionally left; everything else must be gone
            leaked = [p for p in (d.iterdir() if d.exists() else [])]
            if len(leaked) > died:
                tally.leaked_worktrees += len(leaked) - died
        else:
            if d.exists() and any(d.iterdir()):
                tally.leaked_worktrees += 1
    finally:
        conn.close()


def parallel_runs(db_url, gws, n, tally: Tally):
    conn = connect(db_url)
    try:
        runs = [
            runs_mod.create_run_from_dag(conn, diamond(), budgets=budgets(), capabilities=set(CAPS), pool="stress")
            for _ in range(n)
        ]
        stop = threading.Event()
        agent = StubAgent({"sleep_s": 0.05})
        ws = [
            Worker(f"svc{i}", list(CAPS), agent, database_url=db_url, poll_s=0.02, workspace=gws, pools=["stress"])
            for i in range(8)
        ]
        threads = [run_worker_thread(w, stop) for w in ws]
        reaped: list = []

        def reaper():
            c = connect(db_url)
            try:
                while not stop.is_set():
                    try:
                        reaped.extend(leases.reap_expired(c))
                    except Exception:
                        logging.getLogger("stress.reaper").exception("reaper failed")
                        c.rollback()
                    time.sleep(0.05)
            finally:
                c.close()

        rt = threading.Thread(target=reaper, daemon=True)
        rt.start()
        t0 = time.monotonic()
        try:
            while True:
                open_ = [r for r in runs if not sm.get_run(conn, r.id).status.terminal]
                if not open_:
                    break
                for r in open_:
                    scheduler.tick(conn, r.id, verifier=StubVerifier(True), workspace=gws)
                if time.monotonic() - t0 > 300:
                    tally.failures.append("parallel: timeout")
                    break
                time.sleep(0.05)
        finally:
            stop.set()
            wait_all(threads, 10)
            rt.join(5)
        tally.seconds += time.monotonic() - t0
        for r in runs:
            tally.runs += 1
            fr = sm.get_run(conn, r.id)
            if fr.status is RunStatus.PASSED:
                tally.passed += 1
            else:
                tally.failures.append(f"{r.id}: {fr.status.value} {fr.verdict}")
            ab = sum(1 for a in sm.attempts_for_run(conn, r.id) if a.status is AttemptStatus.ABANDONED)
            tally.abandoned += ab
            if ab:
                tally.failures.append(f"{r.id}: unexpected abandoned={ab}")
            d = gws.worktree_root / str(r.id)
            if d.exists() and any(d.iterdir()):
                tally.leaked_worktrees += 1
        tally.stale += sum(w.stats.stale for w in ws)
        if reaped:
            tally.failures.append(f"parallel: reaper reaped {len(reaped)} live attempts")
    finally:
        conn.close()


def _progress(name: str, i: int, n: int, t: Tally, counter: Counter) -> None:
    print(
        f"  {name} {i}/{n}  passed={t.passed} died={t.died} abandoned={t.abandoned} stale={t.stale} "
        f"deadlocks={counter.deadlocks}",
        flush=True,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--diamonds", type=int, default=100)
    ap.add_argument("--chaos", type=int, default=50)
    ap.add_argument("--parallel", type=int, default=20)
    ap.add_argument("--db", default=os.environ.get("MAS_DATABASE_URL", "postgresql://mas:mas@localhost:5432/mas"))
    args = ap.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    counter = Counter()
    logging.getLogger().addHandler(counter)

    dbname = f"mas_stress_{uuid.uuid4().hex[:8]}"
    admin = psycopg.connect(make_conninfo(args.db, dbname="postgres"), autocommit=True)
    admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    db_url = make_conninfo(args.db, dbname=dbname)
    root = Path(tempfile.mkdtemp(prefix="mas_stress_"))
    gws = GitWorkspace(root / "repos", root / "worktrees")
    t_all = time.monotonic()
    try:
        with connect(db_url) as c:
            migrate(c)
        results: dict[str, Tally] = {}

        t = Tally()
        for i in range(args.diamonds):
            one_run(db_url, gws, chaos=False, tally=t)
            if (i + 1) % 10 == 0:
                _progress("diamonds", i + 1, args.diamonds, t, counter)
        results["diamonds"] = t

        t = Tally()
        for i in range(args.chaos):
            one_run(db_url, gws, chaos=True, tally=t)
            if (i + 1) % 10 == 0:
                _progress("chaos", i + 1, args.chaos, t, counter)
        results["chaos"] = t

        t = Tally()
        parallel_runs(db_url, gws, args.parallel, t)
        _progress("parallel", args.parallel, args.parallel, t, counter)
        results["parallel"] = t

        print("\n=== step 6 stability gate ===")
        ok = True
        for name, tl in results.items():
            print(
                f"{name:9s} runs={tl.runs} passed={tl.passed} stale={tl.stale} died={tl.died} "
                f"abandoned={tl.abandoned} leaked_worktrees={tl.leaked_worktrees} time={tl.seconds:.1f}s"
            )
            for f in tl.failures[:10]:
                print("    !", f)
        chaos_ok = results["chaos"].abandoned == results["chaos"].died and results["chaos"].died > 0
        checks = {
            "zero deadlocks": counter.deadlocks == 0,
            "all runs PASSED": all(tl.passed == tl.runs for tl in results.values()),
            "zero stale reports": all(tl.stale == 0 for tl in results.values()),
            "no abandonments outside chaos": results["diamonds"].abandoned == 0 and results["parallel"].abandoned == 0,
            "exactly one abandonment per deliberate death": chaos_ok,
            "zero leaked worktrees": all(tl.leaked_worktrees == 0 for tl in results.values()),
            "no error logs": not counter.errors,
        }
        for k, v in checks.items():
            print(f"  [{'PASS' if v else 'FAIL'}] {k}")
            ok = ok and v
        if counter.errors:
            for e in counter.errors[:10]:
                print("    error:", e)
        print(f"total {time.monotonic() - t_all:.0f}s -> {'GATE PASSED' if ok else 'GATE FAILED'}")
        return 0 if ok else 1
    finally:
        try:
            admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(dbname)))
        finally:
            admin.close()
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
