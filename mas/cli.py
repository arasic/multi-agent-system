"""`mas` command line.

mas migrate                              apply schema migrations
mas run --dag FILE [--workers N ...]     in-process run: orchestrator + N stub workers (dev/demo)
mas submit --dag FILE [--wait]           create a run for the orchestrator/worker services to execute
mas orchestrate --watch [--verifier external|acceptance|stub] [--parallel N]   orchestrator service (bounded, concurrent)
mas verify --watch | --once              verifier service: real sandboxed verdicts for runs left in VERIFYING
mas worker --stub [--capabilities ...]   worker service (compose); --model <provider>:<model> attaches a metered model
mas models [--ping]                      configured model roles, pricing status; --ping makes one metered test call
mas status RUN_ID                        summary + metrics (+ pending questions when AWAITING_INPUT)
mas answer RUN_ID "text"                 answer the planner's clarifying questions (ADR-006)
mas artifacts RUN_ID                     list artifacts (git_commit shas, sha:path documents, decisions, verification)
mas contract FILE                        validate an acceptance contract against the trusted adapters (ADR-007)
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
from pathlib import Path
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
from mas.verifier.acceptance import AcceptanceVerifier, SandboxLimits
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
    provider = _worker_provider(None)  # $MAS_MODEL_WORKER; stub agents ignore it, LLM agents (step 10) use ctx.model
    ws = _workspace(args.workspace)
    print(
        f"  workspace={ws.name}"
        + (f"  repos={settings().repo_root}  worktrees={settings().worktree_root}" if ws.name == "git" else "")
    )
    workers = [
        Worker(f"worker-{i + 1}", sorted(caps), agent, poll_s=0.1, run_id=run.id, workspace=ws, provider=provider)
        for i in range(args.workers)
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

    verifier = StubVerifier(passed=not args.verifier_fail) if args.stub_verifier or args.verifier_fail else _acceptance_verifier()
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
    if m.model_calls:
        unpriced = f"  UNPRICED={m.unpriced_calls} (cost understated; set MAS_MODEL_PRICES)" if m.unpriced_calls else ""
        print(
            f"  model calls={m.model_calls} errors={m.model_call_errors} "
            f"tokens in/out={m.call_input_tokens}/{m.call_output_tokens} cache_read={m.call_cache_read_tokens} "
            f"cost=${m.call_cost_usd} time={m.call_seconds}s{unpriced}"
        )
        for k, v in m.per_model.items():
            print(
                f"    {k:36s} calls={v['calls']} err={v['errors']} in/out={v['input_tokens']}/{v['output_tokens']} "
                f"cost=${v['cost_usd']} {v['seconds']}s" + ("  unpriced" if v["unpriced"] else "")
            )


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


def _service_verifier(kind: str):
    """acceptance = real sandboxed verifier (needs Docker); external = leave runs in VERIFYING for `mas verify`;
    stub = explicit test mode only."""
    from mas.verifier.base import DeferredVerification

    if kind == "stub":
        return StubVerifier(passed=True)
    if kind == "external":
        return DeferredVerification()
    return _acceptance_verifier()


def cmd_orchestrate(args: argparse.Namespace) -> int:
    conn = connect()
    migrate(conn)
    kind = "stub" if args.stub_verifier else args.verifier
    verifier = _service_verifier(kind)
    if args.run:
        final = scheduler.run_until_terminal(conn, UUID(args.run), verifier=verifier, tick_s=args.tick_s)
        print(f"{final.status.value} verdict={final.verdict}")
        return 0 if final.status.value == "PASSED" else 1
    pools = _pools(args.pool)
    ws = _workspace(None)
    conn.close()
    print(
        f"orchestrator: watching open runs in pools={pools} workspace={ws.name} verifier={kind} "
        f"parallel={args.parallel} (Ctrl-C to stop)"
    )
    stop = threading.Event()
    try:
        scheduler.orchestrate_forever(
            settings().database_url,
            verifier=verifier,
            tick_s=args.tick_s,
            stop=stop,
            pools=pools,
            workspace=ws,
            max_parallel=args.parallel,
        )
    except KeyboardInterrupt:
        stop.set()
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    """Verifier service: claims runs in VERIFYING (left there by `mas orchestrate --verifier external`) and produces
    real, sandboxed verdicts. Runs where the sandbox runner (Docker) is available — typically the host."""
    pools = _pools(args.pool)
    ws = _workspace(None)
    verifier = StubVerifier(passed=True) if args.stub_verifier else _acceptance_verifier()
    if args.once:
        conn = connect()
        migrate(conn)
        results = scheduler.verify_once(conn, verifier=verifier, workspace=ws, pools=pools)
        conn.close()
        for rid, status in results:
            print(f"  {rid} -> {status.value}")
        print(f"verified {len(results)} run(s)")
        return 0
    print(
        f"verifier: watching VERIFYING runs in pools={pools} verifier={getattr(verifier, 'name', '?')} "
        f"parallel={args.parallel} (Ctrl-C to stop)"
    )
    stop = threading.Event()
    try:
        scheduler.verify_forever(
            settings().database_url,
            verifier=verifier,
            tick_s=args.tick_s,
            stop=stop,
            pools=pools,
            workspace=ws,
            max_parallel=args.parallel,
        )
    except KeyboardInterrupt:
        stop.set()
    return 0


def _acceptance_verifier() -> AcceptanceVerifier:
    s = settings()
    return AcceptanceVerifier(
        s.acceptance_root,
        image=s.verifier_image,
        limits=SandboxLimits(
            timeout_s=s.verifier_timeout_s,
            cpus=s.verifier_cpus,
            memory_mb=s.verifier_memory_mb,
            pids=s.verifier_pids,
        ),
    )


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


def cmd_contract(args: argparse.Namespace) -> int:
    """Validate an ADR-007 acceptance contract with the trusted adapter schema; print its check ids and, if the
    contract sits inside an acceptance suite dir, the suite digest a freeze would pin. Unmappable → exit 2."""
    from mas.verifier.acceptance import AcceptanceVerifier, InvalidSuite
    from mas.verifier.adapters import InvalidContract, parse_contract

    path = Path(args.file)
    try:
        contract = parse_contract(json.loads(path.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError) as e:
        print(f"cannot read contract: {e}", file=sys.stderr)
        return 2
    except InvalidContract as e:
        print(f"UNMAPPABLE: {e}", file=sys.stderr)
        return 2
    print(f"contract ok: {len(contract.checks)} check(s): {contract.check_ids}")
    if contract.service:
        print(f"  service: {' '.join(contract.service.start)}  health={contract.service.health} port={contract.service.port}")
    suite_dir = path.parent
    if (suite_dir / "suite.json").exists():
        try:
            digest = AcceptanceVerifier(suite_dir.parent).suite_digest(suite_dir.name)
        except InvalidSuite as e:
            print(f"suite invalid: {e}", file=sys.stderr)
            return 2
        print(f"  suite {suite_dir.name}: sha256={digest}  (this is what an approved contract pins)")
    return 0


def _pools(arg: str | None) -> list[str] | None:
    """--pool 'a,b' → ['a','b']; --pool '*' → None (serve every pool); default → [MAS_POOL]."""
    raw = arg if arg is not None else settings().pool
    if raw.strip() == "*":
        return None
    return [p.strip() for p in raw.split(",") if p.strip()]


def _worker_provider(spec: str | None):
    """--model <provider>:<model> or $MAS_MODEL_WORKER; None = no model (stub agents). Only mas/providers names vendors."""
    from mas import providers

    spec = spec if spec is not None else settings().model_worker
    return providers.from_spec(spec) if spec else None


def cmd_worker(args: argparse.Namespace) -> int:
    if not args.stub:
        print("only --stub workers exist until roadmap step 10 (the model layer is in; the LLM agent is next)", file=sys.stderr)
        return 2
    caps = [c.strip() for c in (args.capabilities or ",".join(settings().worker_capabilities)).split(",") if c.strip()]
    wid = args.id or f"worker-{socket.gethostname()}-{os.getpid()}"
    pools = _pools(args.pool)
    ws = _workspace(args.workspace)
    provider = _worker_provider(args.model)
    w = Worker(
        wid,
        caps,
        StubAgent(default_script={"sleep_s": args.stub_sleep}),
        poll_s=settings().worker_poll_s,
        pools=pools,
        workspace=ws,
        provider=provider,
    )
    model = f"{provider.name}:{provider.model}" if provider else "none"
    print(f"{wid}: capabilities={caps} pools={pools} model={model} (Ctrl-C to stop)")
    stop = threading.Event()
    try:
        w.run_forever(stop)
    except KeyboardInterrupt:
        stop.set()
    return 0


def cmd_models(args: argparse.Namespace) -> int:
    """Show the configured model roles and pricing status; --ping makes one small metered call per configured role
    (or the given --spec) and prints the telemetry record — the smallest end-to-end proof that a provider works."""
    from mas import providers

    cfg = settings()
    pricing = providers.pricing_from_settings(cfg)
    known = f": {pricing.known_models()}" if len(pricing) else ""
    print(f"pricing: {len(pricing)} model(s) configured via MAS_MODEL_PRICES{known}")
    specs: list[tuple[str, str]] = []
    if args.spec:
        specs.append(("adhoc", args.spec))
    else:
        for role in providers.ROLES:
            spec = providers.role_spec(role, cfg)
            flag = "" if not spec or pricing.price(providers.parse_spec(spec)[1]) else "  [unpriced]"
            print(f"  {role:9s} {spec or '(none)'}{flag}")
            if spec:
                specs.append((role, spec))
    print(f"  attempt budget: max_calls={cfg.attempt_max_calls} max_tokens={cfg.attempt_max_tokens}")
    if not args.ping:
        return 0
    if not specs:
        print("nothing to ping: set MAS_MODEL_WORKER / MAS_MODEL_PLANNER or pass --spec <provider>:<model>", file=sys.stderr)
        return 2
    rc = 0
    for role, spec in specs:
        sink = providers.MemorySink()
        try:
            base = providers.from_spec(spec, cfg=cfg)
            m = providers.meter(base, role="ping", sink=sink, pricing=pricing, budget=providers.CallBudget(max_calls=1))
            comp = m.complete([{"role": "user", "content": args.prompt}], max_tokens=args.max_tokens)
            print(f"[{role}] {spec}: {comp.stop_reason} {comp.text.strip()[:200]!r}")
        except Exception as e:  # noqa: BLE001 - diagnostic command: surface anything
            print(f"[{role}] {spec}: FAILED {type(e).__name__}: {e}", file=sys.stderr)
            rc = 1
        for rec in sink.records:
            print("   ", json.dumps(rec.as_dict(), default=str))
    return rc


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
    r.add_argument(
        "--stub-verifier",
        action="store_true",
        help="explicit test mode: use the passing stub instead of the fixed acceptance suite",
    )
    r.add_argument("--timeout", type=float, default=None, help="client-side guard; default max_wallclock_s + 60")
    r.add_argument(
        "--ask", default=None, help="ADR-006 demo: planner asks these ';'-separated questions first; answer via `mas answer`"
    )
    r.add_argument("--workspace", default=None, choices=["git", "none"], help="git worktrees (default) or no filesystem")
    r.set_defaults(fn=cmd_run)

    ct = sub.add_parser("contract", help="validate an ADR-007 acceptance contract (trusted adapters only)")
    ct.add_argument("file", help="path to contract.json (inside an acceptance suite dir to also get the suite digest)")
    ct.set_defaults(fn=cmd_contract)

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
    o.add_argument(
        "--verifier",
        default="acceptance",
        choices=["acceptance", "external", "stub"],
        help="acceptance = sandboxed verifier in this process (needs Docker); external = leave runs in VERIFYING for "
        "`mas verify --watch`; stub = explicit test mode only",
    )
    o.add_argument("--stub-verifier", action="store_true", help="alias for --verifier stub (explicit test mode only)")
    o.add_argument("--parallel", type=int, default=4, help="max runs ticked concurrently (bounded executor)")
    o.set_defaults(fn=cmd_orchestrate)

    vf = sub.add_parser("verify", help="verifier service: real sandboxed verdicts for runs left in VERIFYING")
    vf.add_argument("--watch", action="store_true")
    vf.add_argument("--once", action="store_true", help="verify all currently VERIFYING runs once and exit")
    vf.add_argument("--tick-s", type=float, default=settings().orchestrator_tick_s)
    vf.add_argument("--pool", default=None, help="comma-separated pools to serve; '*' = all (default: $MAS_POOL or 'default')")
    vf.add_argument("--parallel", type=int, default=2, help="max concurrent verifications (each is a sandbox)")
    vf.add_argument("--stub-verifier", action="store_true", help="explicit test mode only")
    vf.set_defaults(fn=cmd_verify)

    w = sub.add_parser("worker", help="worker service")
    w.add_argument("--stub", action="store_true")
    w.add_argument(
        "--model",
        default=None,
        help="<provider>:<model> handed to agents as a metered ctx.model (default: $MAS_MODEL_WORKER; empty = none)",
    )
    w.add_argument("--capabilities", default=None)
    w.add_argument("--id", default=None)
    w.add_argument("--stub-sleep", type=float, default=0.5)
    w.add_argument("--pool", default=None, help="comma-separated pools to serve; '*' = all (default: $MAS_POOL or 'default')")
    w.add_argument("--workspace", default=None, choices=["git", "none"])
    w.set_defaults(fn=cmd_worker)

    md = sub.add_parser("models", help="configured model roles + pricing status; --ping makes one metered test call")
    md.add_argument("--ping", action="store_true", help="make one small metered call per configured role (or --spec)")
    md.add_argument("--spec", default=None, help="ad-hoc <provider>:<model> to ping instead of the configured roles")
    md.add_argument("--prompt", default="Reply with the single word OK.", help="prompt used by --ping")
    md.add_argument("--max-tokens", type=int, default=64)
    md.set_defaults(fn=cmd_models)

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
