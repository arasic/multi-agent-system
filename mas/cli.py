"""`mas` command line.

mas migrate                              apply schema migrations
mas run --dag FILE [--workers N ...]     in-process run: orchestrator + N stub workers (dev/demo)
mas submit --dag FILE [--wait]           create a run for the orchestrator/worker services to execute
mas orchestrate --watch | --run ID       orchestrator service (compose) or tick one run to terminal
mas worker --stub [--capabilities ...]   worker service (compose)
mas status RUN_ID                        summary + metrics (+ pending questions when AWAITING_INPUT)
mas answer RUN_ID "text"                 answer the planner's clarifying questions (ADR-006)
mas artifacts RUN_ID                     list artifacts (git_commit shas, sha:path documents, decisions, verification)
mas replay RUN_ID                        event timeline (invariant I-12)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import sys
import threading
import time
from uuid import UUID

from mas import metrics
from mas.config import settings
from mas.db import connect, migrate
from mas.db.events import for_run
from mas.models.types import Budgets
from mas.orchestrator import runs as runs_mod
from mas.orchestrator import scheduler
from mas.planner.dag import DagSpec
from mas.planner.planner import StubPlanner
from mas.verifier.stub import StubVerifier
from mas.workers.runtime import Worker, run_worker_thread
from mas.workers.stub import StubAgent


def _log(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )


def cmd_migrate(args: argparse.Namespace) -> int:
    with connect() as conn:
        applied = migrate(conn)
    print(f"migrations applied: {applied or 'none (up to date)'}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    dag = DagSpec.from_file(args.dag)
    budgets = Budgets(
        max_concurrency=args.max_concurrency,
        lease_s=args.lease_s,
        max_wallclock_s=args.max_wallclock_s,
        max_attempts_per_task=args.max_attempts,
        max_attempt_runtime_s=args.max_attempt_runtime_s,
    )
    caps = set(settings().worker_capabilities)
    conn = connect()
    migrate(conn)
    # in-process runs live in their own pool so long-running services on the same DB leave them alone
    pool = f"local:{os.getpid()}"
    planner = None
    if args.ask:
        # ADR-006 demo: a stub planner that asks first; answer from another terminal with `mas answer <run_id> "..."`
        planner = StubPlanner(dag, questions=[[q.strip() for q in args.ask.split(";") if q.strip()]])
        run = runs_mod.create_run(
            conn,
            goal=args.goal or dag.goal or "(no goal)",
            budgets=budgets,
            benchmark=args.benchmark,
            config=args.config,
            pool=pool,
        )
        run = runs_mod.plan_run(conn, run.id, planner, capabilities=caps)
        print(f"run {run.id}  status={run.status.value}  pool={pool}")
        for i, q in enumerate(runs_mod.pending_questions(conn, run.id), 1):
            print(f"  Q{i}: {q}")
        print(
            f'  -> answer with:  mas answer {run.id} "your answer"   (clock is running: max_wallclock_s={args.max_wallclock_s})'
        )
    else:
        run = runs_mod.create_run_from_dag(
            conn,
            dag,
            goal=args.goal,
            budgets=budgets,
            benchmark=args.benchmark,
            config=args.config,
            capabilities=caps,
            pool=pool,
        )
        print(
            f"run {run.id}  ({len(dag.tasks)} tasks, {args.workers} workers, max_concurrency={args.max_concurrency}, pool={pool})"
        )

    stop = threading.Event()
    agent = StubAgent(default_script={"sleep_s": args.stub_sleep})
    ws = _workspace(args.workspace)
    print(
        f"  workspace={ws.name}"
        + (f"  repos={settings().repo_root}  worktrees={settings().worktree_root}" if ws.name == "git" else "")
    )
    workers = [
        Worker(f"worker-{i + 1}", sorted(caps), agent, poll_s=0.1, run_id=run.id, workspace=ws) for i in range(args.workers)
    ]
    threads = [run_worker_thread(w, stop) for w in workers]

    if args.chaos_kill_after is not None:

        def _chaos() -> None:
            time.sleep(args.chaos_kill_after)
            # kill a worker that is mid-attempt (wait up to 5s for one), so the reaper has something to recover
            victim = None
            deadline = time.monotonic() + 5
            while victim is None and time.monotonic() < deadline:
                victim = next((w for w in workers if w.busy), None)
                if victim is None:
                    time.sleep(0.05)
            victim = victim or workers[0]
            task = victim.current.task.key if victim.current else "(idle)"
            victim.die()
            print(f"[chaos] killed {victim.worker_id} at t+{args.chaos_kill_after}s while on {task}")

        threading.Thread(target=_chaos, daemon=True).start()

    verifier = StubVerifier(passed=not args.verifier_fail)
    timeout = args.timeout if args.timeout is not None else args.max_wallclock_s + 60
    t0 = time.monotonic()
    try:
        final = scheduler.run_until_terminal(
            conn, run.id, verifier=verifier, planner=planner, capabilities=caps, workspace=ws, tick_s=0.2, timeout_s=timeout
        )
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=5)
    elapsed = time.monotonic() - t0
    m = metrics.compute(conn, run.id)
    print(f"\n{final.status.value}  verdict={final.verdict}  in {elapsed:.2f}s")
    _print_metrics(m)
    for w in workers:
        s = w.stats
        print(
            f"  {w.worker_id}: claimed={s.claimed} completed={s.completed} failed={s.failed} "
            f"stale={s.stale} died={s.died}  tasks={s.tasks}"
        )
    conn.close()
    return 0 if final.status.value == "PASSED" else 1


def _print_metrics(m: metrics.RunMetrics) -> None:
    print(
        f"  tasks={m.tasks} {m.tasks_by_status}\n"
        f"  attempts={m.attempts} {m.attempts_by_status}  retries={m.retries} abandoned={m.abandoned} timeouts={m.timeouts}\n"
        f"  wall_clock={m.wall_clock_s}s  sum_attempt={m.sum_attempt_s}s  "
        f"max_concurrent={m.max_concurrent_attempts}  parallelism_eff={m.parallelism_efficiency}\n"
        f"  total={m.total_s}s  machine={m.machine_s}s  human_wait={m.human_wait_s}s  questions={m.questions}\n"
        f"  tokens in/out={m.input_tokens}/{m.output_tokens} cost=${m.cost_usd}  events={m.events}"
    )
    for k, v in m.per_task.items():
        print(f"    {k:14s} {v['status']:10s} attempts={v['attempts']} {v['seconds']:.2f}s workers={v['workers']}")


def cmd_submit(args: argparse.Namespace) -> int:
    """Create a run and exit; the compose `orchestrator` + `worker` services execute it."""
    dag = DagSpec.from_file(args.dag)
    budgets = Budgets(
        max_concurrency=args.max_concurrency,
        lease_s=args.lease_s,
        max_wallclock_s=args.max_wallclock_s,
        max_attempts_per_task=args.max_attempts,
        max_attempt_runtime_s=args.max_attempt_runtime_s,
    )
    conn = connect()
    migrate(conn)
    run = runs_mod.create_run_from_dag(
        conn,
        dag,
        goal=args.goal,
        budgets=budgets,
        benchmark=args.benchmark,
        config=args.config,
        capabilities=set(settings().worker_capabilities),
        pool=args.pool,
    )
    print(f"submitted run {run.id} ({len(dag.tasks)} tasks) status={run.status.value} pool={args.pool}")
    if not args.wait:
        conn.close()
        return 0
    t0 = time.monotonic()
    while True:
        cur = runs_mod.sm.get_run(conn, run.id)
        if cur.status.terminal:
            break
        if time.monotonic() - t0 > args.timeout:
            print(f"timeout: run still {cur.status.value}")
            conn.close()
            return 3
        time.sleep(0.5)
    m = metrics.compute(conn, run.id)
    print(f"{cur.status.value}  verdict={cur.verdict}")
    _print_metrics(m)
    conn.close()
    return 0 if cur.status.value == "PASSED" else 1


def cmd_orchestrate(args: argparse.Namespace) -> int:
    conn = connect()
    migrate(conn)
    verifier = StubVerifier(passed=True)
    if args.run:
        final = scheduler.run_until_terminal(conn, UUID(args.run), verifier=verifier, tick_s=args.tick_s)
        print(f"{final.status.value} verdict={final.verdict}")
        return 0 if final.status.value == "PASSED" else 1
    pools = _pools(args.pool)
    ws = _workspace(None)
    print(f"orchestrator: watching open runs in pools={pools} workspace={ws.name} (Ctrl-C to stop)")
    stop = threading.Event()
    try:
        scheduler.orchestrate_forever(conn, verifier=verifier, tick_s=args.tick_s, stop=stop, pools=pools, workspace=ws)
    except KeyboardInterrupt:
        pass
    return 0


def _workspace(arg: str | None):
    """--workspace git|none (default: $MAS_WORKSPACE or git). Falls back to none if git is unavailable."""
    from mas.workers.workspace import GitWorkspace, NullWorkspace, git_available

    kind = (arg or settings().workspace).lower()
    if kind == "git":
        if not git_available():
            print("git not found on PATH; falling back to --workspace none", file=sys.stderr)
            return NullWorkspace()
        s = settings()
        return GitWorkspace(s.repo_root, s.worktree_root, keep_worktrees=s.keep_worktrees)
    return NullWorkspace()


def cmd_artifacts(args: argparse.Namespace) -> int:
    """List a run's artifacts: type, status, ref, task/attempt — the audit view of what was produced."""
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT ar.type, ar.status, ar.ref, t.key, a.attempt_number, ar.created_at
            FROM artifacts ar LEFT JOIN tasks t ON t.id = ar.task_id LEFT JOIN attempts a ON a.id = ar.attempt_id
            WHERE ar.run_id = %s ORDER BY ar.created_at, ar.id
            """,
            (UUID(args.run_id),),
        ).fetchall()
    if not rows:
        print("no artifacts")
        return 1
    for r in rows:
        who = f"{r['key']}#{r['attempt_number']}" if r["key"] else "(run)"
        print(f"  {r['type']:14s} {r['status']:10s} {who:12s} {r['ref']}")
    return 0


def _pools(arg: str | None) -> list[str] | None:
    """--pool 'a,b' → ['a','b']; --pool '*' → None (serve every pool); default → [MAS_POOL]."""
    raw = arg if arg is not None else settings().pool
    if raw.strip() == "*":
        return None
    return [p.strip() for p in raw.split(",") if p.strip()]


def cmd_worker(args: argparse.Namespace) -> int:
    if not args.stub:
        print("only --stub workers exist until roadmap step 10", file=sys.stderr)
        return 2
    caps = [c.strip() for c in (args.capabilities or ",".join(settings().worker_capabilities)).split(",") if c.strip()]
    wid = args.id or f"worker-{socket.gethostname()}-{os.getpid()}"
    pools = _pools(args.pool)
    ws = _workspace(args.workspace)
    w = Worker(
        wid,
        caps,
        StubAgent(default_script={"sleep_s": args.stub_sleep}),
        poll_s=settings().worker_poll_s,
        pools=pools,
        workspace=ws,
    )
    print(f"{wid}: capabilities={caps} pools={pools} (Ctrl-C to stop)")
    stop = threading.Event()
    try:
        w.run_forever(stop)
    except KeyboardInterrupt:
        stop.set()
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    with connect() as conn:
        m = metrics.compute(conn, UUID(args.run_id))
        report = scheduler.stall_report(conn, UUID(args.run_id)) if m.status not in {"PASSED", "FAILED", "ABORTED"} else None
    if args.json:
        d = m.as_dict()
        if report:
            d["open"] = report
        print(json.dumps(d, indent=2, default=str))
    else:
        print(f"{m.status}  verdict={m.verdict}")
        _print_metrics(m)
        if report:
            if report.get("pending_questions"):
                print("  AWAITING_INPUT - the planner asked:")
                for i, q in enumerate(report["pending_questions"], 1):
                    print(f"    Q{i}: {q}")
                print(f'  -> mas answer {args.run_id} "your answer"')
            else:
                print(f"  open: {json.dumps(report, default=str)}")
    return 0


def cmd_answer(args: argparse.Namespace) -> int:
    """Record the human's answer to a run's pending question batch (ADR-006)."""
    with connect() as conn:
        try:
            run = runs_mod.answer(conn, UUID(args.run_id), args.text, by=args.by)
        except runs_mod.NotAwaitingInput as e:
            print(str(e), file=sys.stderr)
            return 2
    print(f"answer recorded; run {run.id} -> {run.status.value}")
    return 0


def cmd_replay(args: argparse.Namespace) -> int:
    with connect() as conn:
        evs = for_run(conn, UUID(args.run_id))
    if not evs:
        print("no events")
        return 1
    t0 = evs[0].ts
    for e in evs:
        dt = (e.ts - t0).total_seconds()
        who = f" [{e.worker_id}]" if e.worker_id else ""
        key = e.payload.get("key")
        keys = f" {key}" if key else ""
        extra = {k: v for k, v in e.payload.items() if k not in {"key", "from"} and v not in (None, {}, [])}
        frm = f" (from {e.payload['from']})" if "from" in e.payload else ""
        print(f"+{dt:8.3f}s  {e.type:22s}{keys}{frm}{who}  {json.dumps(extra, default=str) if extra and args.verbose else ''}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="mas", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("migrate", help="apply schema migrations").set_defaults(fn=cmd_migrate)

    r = sub.add_parser("run", help="in-process run with stub workers")
    r.add_argument("--dag", required=True, help="hand-written DAG JSON file")
    r.add_argument("--goal", default=None)
    r.add_argument("--benchmark", default=None)
    r.add_argument("--config", default="D", help="A|B|C|D (evaluation.md)")
    r.add_argument("--workers", type=int, default=3)
    r.add_argument("--max-concurrency", type=int, default=4)
    r.add_argument("--max-attempts", type=int, default=3)
    r.add_argument("--lease-s", type=int, default=5)
    r.add_argument("--max-wallclock-s", type=int, default=300, help="run budget; the run ABORTS with a verdict when exceeded")
    r.add_argument("--max-attempt-runtime-s", type=int, default=120)
    r.add_argument("--stub-sleep", type=float, default=0.5, help="simulated work per attempt (s)")
    r.add_argument("--chaos-kill-after", type=float, default=None, help="kill a busy worker after N seconds (A5 demo)")
    r.add_argument("--verifier-fail", action="store_true", help="stub verifier returns FAIL")
    r.add_argument("--timeout", type=float, default=None, help="client-side guard; default max_wallclock_s + 60")
    r.add_argument(
        "--ask", default=None, help="ADR-006 demo: planner asks these ';'-separated questions first; answer via `mas answer`"
    )
    r.add_argument("--workspace", default=None, choices=["git", "none"], help="git worktrees (default) or no filesystem")
    r.set_defaults(fn=cmd_run)

    ar = sub.add_parser("artifacts", help="list a run's artifacts (type, status, ref, task#attempt)")
    ar.add_argument("run_id")
    ar.set_defaults(fn=cmd_artifacts)

    an = sub.add_parser("answer", help="answer a run's pending clarifying questions (ADR-006)")
    an.add_argument("run_id")
    an.add_argument("text")
    an.add_argument("--by", default="human")
    an.set_defaults(fn=cmd_answer)

    sb = sub.add_parser("submit", help="create a run from a DAG file and exit (services pick it up)")
    sb.add_argument("--dag", required=True)
    sb.add_argument("--goal", default=None)
    sb.add_argument("--benchmark", default=None)
    sb.add_argument("--config", default="D")
    sb.add_argument("--max-concurrency", type=int, default=4)
    sb.add_argument("--max-attempts", type=int, default=3)
    sb.add_argument("--lease-s", type=int, default=15)
    sb.add_argument("--max-wallclock-s", type=int, default=600)
    sb.add_argument("--max-attempt-runtime-s", type=int, default=120)
    sb.add_argument("--wait", action="store_true", help="block until the run is terminal, then print status")
    sb.add_argument("--timeout", type=float, default=600)
    sb.add_argument("--pool", default="default", help="pool the services must serve to pick this run up")
    sb.set_defaults(fn=cmd_submit)

    o = sub.add_parser("orchestrate", help="orchestrator service / tick a run")
    o.add_argument("--watch", action="store_true")
    o.add_argument("--run", default=None)
    o.add_argument("--tick-s", type=float, default=settings().orchestrator_tick_s)
    o.add_argument("--pool", default=None, help="comma-separated pools to serve; '*' = all (default: $MAS_POOL or 'default')")
    o.set_defaults(fn=cmd_orchestrate)

    w = sub.add_parser("worker", help="worker service")
    w.add_argument("--stub", action="store_true")
    w.add_argument("--capabilities", default=None)
    w.add_argument("--id", default=None)
    w.add_argument("--stub-sleep", type=float, default=0.5)
    w.add_argument("--pool", default=None, help="comma-separated pools to serve; '*' = all (default: $MAS_POOL or 'default')")
    w.add_argument("--workspace", default=None, choices=["git", "none"])
    w.set_defaults(fn=cmd_worker)

    s = sub.add_parser("status", help="run summary + metrics")
    s.add_argument("run_id")
    s.add_argument("--json", action="store_true")
    s.set_defaults(fn=cmd_status)

    rp = sub.add_parser("replay", help="event timeline")
    rp.add_argument("run_id")
    rp.set_defaults(fn=cmd_replay)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _log(args.verbose)
    return int(args.fn(args))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
