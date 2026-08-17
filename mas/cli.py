"""`mas` command line.

mas migrate                              apply schema migrations
mas run --dag FILE | --goal TEXT --planner llm|fake [--workers N ...]   in-process run (dev/demo)
mas submit --dag FILE [--wait]           create a run for the orchestrator/worker services to execute
mas orchestrate --watch [--verifier external|acceptance|stub] [--parallel N]   orchestrator service (bounded, concurrent)
mas verify --watch | --once              verifier service: real sandboxed verdicts for runs left in VERIFYING
mas execute --watch | --once             execution-runner service: workers' command requests -> per-attempt sandboxes
mas worker [--agent stub|llm] [--model <provider>:<model>] [--exec-backend sandbox|none]   worker service (compose)
mas models [--ping]                      configured model roles, pricing status; --ping makes one metered test call
mas gateway [--upstream P:M]             model gateway: the one process holding a vendor key (compose: workers -> gateway)
mas doctor [--require-live]               preflight Docker, Postgres, images, workspace and model configuration
mas up [--workers N]                      Compose services + trusted host execution/verifier supervisor
mas down                                  stop the Compose services (data is retained unless --volumes)
mas status RUN_ID                        summary + metrics (+ pending questions when AWAITING_INPUT)
mas result RUN_ID [--output DIR]          show or export the exact verified repository commit
mas answer RUN_ID "text"                 answer the planner's clarifying questions (ADR-006)
mas approve RUN_ID [--contract f.json]   approve the planner's acceptance-contract proposal -> frozen definition of done (ADR-007)
mas artifacts RUN_ID                     list artifacts (git_commit shas, sha:path documents, decisions, verification)
mas contract FILE                        validate an acceptance contract against the trusted adapters (ADR-007)
mas replay RUN_ID                        event timeline (invariant I-12)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any
from uuid import UUID

from mas import metrics
from mas.config import settings
from mas.db import connect, migrate
from mas.db.events import for_run
from mas.evaluation import (
    CONFIGS,
    SingleAgentRepairPlanner,
    dag_for_config,
    effective_concurrency,
    normalize_config,
    single_agent_dag,
)
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


def _command_ok(argv: list[str], *, timeout: float = 30) -> tuple[bool, str]:
    try:
        p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, str(exc)
    detail = (p.stdout or p.stderr or "").strip().splitlines()
    return p.returncode == 0, (detail[0][:300] if detail else f"exit {p.returncode}")


def _dotenv_values(path: Path = Path(".env")) -> dict[str, str]:
    """Minimal Compose-compatible KEY=VALUE reader for diagnostics; never mutates os.environ."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}
    out: dict[str, str] = {}
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key and key.replace("_", "").isalnum():
            out[key] = value.strip().strip('"').strip("'")
    return out


def _doctor_checks(*, require_live: bool = False) -> list[dict[str, Any]]:
    """Read-only MVP preflight. Each row is stable enough for both the human output and `--json`."""
    cfg = settings()
    dot = _dotenv_values()
    value = lambda name, fallback="": os.environ.get(name) or dot.get(name) or fallback  # noqa: E731
    rows: list[dict[str, Any]] = []

    def add(name: str, ok: bool, detail: str, *, required: bool = True) -> None:
        rows.append({"name": name, "ok": bool(ok), "required": required, "detail": detail})

    add("python", sys.version_info >= (3, 12), f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")
    add("git", shutil.which("git") is not None, shutil.which("git") or "not found")
    ok, detail = _command_ok(["docker", "info", "--format", "{{.ServerVersion}}"])
    add("docker", ok, detail)
    ok_image, image_detail = _command_ok(["docker", "image", "inspect", cfg.verifier_image, "--format", "{{.Id}}"])
    add("verifier_image", ok_image, image_detail if ok_image else f"{cfg.verifier_image}: {image_detail}")
    try:
        with connect() as conn:
            version = conn.execute("select current_database() AS db, version() AS version").fetchone()
        add("postgres", True, f"database={version['db']}")
    except Exception as exc:  # noqa: BLE001 - diagnostic boundary
        add("postgres", False, str(exc)[:300])
    for name, raw in (("repo_root", cfg.repo_root), ("worktree_root", cfg.worktree_root)):
        path = Path(raw).resolve()
        parent = next((p for p in (path, *path.parents) if p.exists()), path.parent)
        add(name, os.access(parent, os.W_OK), str(path))

    upstream = value("MAS_GATEWAY_UPSTREAM", cfg.gateway_upstream).strip()
    is_fake = upstream.startswith("fake:") or upstream == "fake"
    add(
        "gateway_upstream",
        bool(upstream) and (not require_live or not is_fake),
        upstream or "not configured",
        required=require_live,
    )
    worker_model = value("MAS_MODEL_WORKER", cfg.model_worker)
    planner_model = value("MAS_MODEL_PLANNER", cfg.model_planner)
    add("worker_model", bool(worker_model), worker_model or "not configured", required=require_live)
    add("planner_model", bool(planner_model), planner_model or "not configured", required=require_live)
    prices = value("MAS_MODEL_PRICES", cfg.model_prices).strip()
    add("model_prices", bool(prices), "configured" if prices else "not configured (cost will be unknown)", required=require_live)
    if require_live:
        provider = upstream.split(":", 1)[0] if upstream else ""
        key_names = {
            "anthropic": ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"),
            "openai": ("MAS_OPENAI_API_KEY", "OPENAI_API_KEY"),
        }.get(provider, ())
        has_key = any(os.environ.get(k) or dot.get(k) for k in key_names)
        add(
            "vendor_credentials",
            bool(key_names) and has_key,
            f"provider={provider or 'unknown'}",
        )
    return rows


def cmd_doctor(args: argparse.Namespace) -> int:
    rows = _doctor_checks(require_live=args.require_live)
    failed = [r for r in rows if r["required"] and not r["ok"]]
    if args.json:
        print(json.dumps({"ok": not failed, "checks": rows}, indent=2))
    else:
        for r in rows:
            mark = "OK" if r["ok"] else ("FAIL" if r["required"] else "WARN")
            print(f"{mark:4s} {r['name']:20s} {r['detail']}")
        print("preflight: " + ("PASS" if not failed else f"FAIL ({len(failed)} required check(s))"))
    return 0 if not failed else 2


def _compose(argv: list[str], *, env: dict[str, str] | None = None) -> int:
    print("+ docker compose " + " ".join(argv), flush=True)
    return subprocess.call(["docker", "compose", *argv], env=env)


def cmd_up(args: argparse.Namespace) -> int:
    """Start the hardened Compose actors and supervise the two trusted host-side services in this foreground process."""
    cfg = settings()
    if not args.offline:
        failed = [r for r in _doctor_checks(require_live=True) if r["required"] and not r["ok"]]
        if failed:
            for r in failed:
                print(f"preflight failed: {r['name']}: {r['detail']}", file=sys.stderr)
            print("run `mas doctor --require-live` for the full report (or use `mas up --offline`)", file=sys.stderr)
            return 2
    env = dict(os.environ)
    dot = _dotenv_values()
    env["MAS_WORKER_AGENT"] = "llm"
    if args.offline:
        env["MAS_GATEWAY_UPSTREAM"] = "fake:builder"
        env["MAS_GATEWAY_MODELS"] = "builder"
        env["MAS_MODEL_WORKER"] = "openai:builder"
        env["MAS_ORCH_PLANNER"] = "fake"
    else:
        env["MAS_ORCH_PLANNER"] = "llm"
        if not env.get("MAS_MODEL_WORKER"):
            env["MAS_MODEL_WORKER"] = dot.get("MAS_MODEL_WORKER") or cfg.model_worker or "openai:builder"
        if not env.get("MAS_MODEL_PLANNER"):
            env["MAS_MODEL_PLANNER"] = dot.get("MAS_MODEL_PLANNER") or cfg.model_planner or "openai:builder"
    if args.build:
        if subprocess.call(["docker", "build", "-f", "acceptance/Dockerfile.verifier", "-t", cfg.verifier_image, "."], env=env):
            return 2
        if _compose(["build", "orchestrator", "gateway", "worker"], env=env):
            return 2
    if _compose(["up", "-d", "--scale", f"worker={args.workers}", "postgres", "orchestrator", "gateway", "worker"], env=env):
        return 2
    host_env = dict(env)
    for secret in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "OPENAI_API_KEY", "MAS_OPENAI_API_KEY", "MAS_GATEWAY_TOKEN"):
        host_env.pop(secret, None)  # only the Compose gateway receives vendor/gateway credentials
    children = [
        subprocess.Popen([sys.executable, "-m", "mas", "execute", "--watch"], env=host_env),
        subprocess.Popen([sys.executable, "-m", "mas", "verify", "--watch"], env=host_env),
    ]
    print("MVP services are ready; executor and verifier are supervised here. Ctrl-C stops the host services.")
    try:
        while True:
            dead = next((p for p in children if p.poll() is not None), None)
            if dead is not None:
                print(f"host service exited unexpectedly with code {dead.returncode}", file=sys.stderr)
                return 1
            time.sleep(0.5)
    except KeyboardInterrupt:
        return 0
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
        for child in children:
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
        if args.down_on_exit:
            _compose(["down"], env=env)


def cmd_down(args: argparse.Namespace) -> int:
    argv = ["down"]
    if args.volumes:
        argv.append("--volumes")
    return 0 if _compose(argv) == 0 else 2


def cmd_run(args: argparse.Namespace) -> int:
    if not args.dag and not args.goal:
        raise SystemExit("mas run needs --dag FILE, or --goal TEXT with --planner llm|fake")
    config = normalize_config(args.config)
    dag = DagSpec.from_file(args.dag) if args.dag else None
    if dag is not None:
        if args.benchmark:
            dag.benchmark = args.benchmark
        dag = dag_for_config(dag, config)
    elif config in {"A", "B"}:
        if not args.goal:
            raise SystemExit("configs A/B need --goal or --dag")
        try:
            dag = single_agent_dag(args.goal, args.benchmark)
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc
    max_concurrency = effective_concurrency(config, args.max_concurrency)
    budgets = Budgets(
        max_concurrency=max_concurrency,
        lease_s=args.lease_s,
        max_wallclock_s=args.max_wallclock_s,
        max_attempts_per_task=args.max_attempts,
        max_attempt_runtime_s=args.max_attempt_runtime_s,
        max_tokens=args.max_tokens,
        max_attempt_tokens=args.max_attempt_tokens,
        max_replans=args.max_replans,
        max_cost_usd=args.max_cost_usd,
    )
    caps = set(settings().worker_capabilities)
    conn = connect()
    migrate(conn)
    # in-process runs live in their own pool so long-running services on the same DB leave them alone
    pool = f"local:{os.getpid()}"
    planner = None
    if dag is None:
        # ad-hoc goal (step 11): the planner asks / assumes, proposes the acceptance contract (approve with `mas approve`),
        # then plans the DAG; every planner output goes through the deterministic driver
        planner = _planner(args.planner or "llm", spec=args.planner_model)
        if planner is None:
            raise SystemExit("--goal needs a planner: --planner llm|fake")
        run = runs_mod.create_run(conn, goal=args.goal, budgets=budgets, benchmark=args.benchmark, config=config, pool=pool)
        run = runs_mod.plan_run(conn, run.id, planner, capabilities=caps)
        print(f"run {run.id}  status={run.status.value}  pool={pool}  planner={planner.name}")
        _print_waiting(conn, run.id)
    elif args.ask:
        # ADR-006 demo: a stub planner that asks first; answer from another terminal with `mas answer <run_id> "..."`
        planner = StubPlanner(dag, questions=[[q.strip() for q in args.ask.split(";") if q.strip()]])
        run = runs_mod.create_run(
            conn,
            goal=args.goal or dag.goal or "(no goal)",
            budgets=budgets,
            benchmark=args.benchmark or dag.benchmark,  # the DAG file's suite; an ad-hoc goal without one needs a contract first
            config=config,
            pool=pool,
        )
        run = runs_mod.plan_run(conn, run.id, planner, capabilities=caps)
        print(f"run {run.id}  status={run.status.value}  pool={pool}")
        _print_waiting(conn, run.id)
    else:
        run = runs_mod.create_run_from_dag(
            conn,
            dag,
            goal=args.goal,
            budgets=budgets,
            benchmark=args.benchmark,
            config=config,
            capabilities=caps,
            pool=pool,
        )
        print(
            f"run {run.id}  (config={config}, {len(dag.tasks)} tasks, {args.workers} workers, "
            f"max_concurrency={max_concurrency}, pool={pool})"
        )
        # bounded repair (13-lite) after a verifier FAIL needs a planner for the amendment; --planner fake|llm opts in
        planner = SingleAgentRepairPlanner() if config in {"A", "B"} else (
            _planner(args.planner, spec=args.planner_model) if args.planner else None
        )
    if args.chaos_kill_after is not None and dag is None:
        raise SystemExit("--chaos-kill-after needs --dag")

    stop = threading.Event()
    provider = _worker_provider(args.model)  # $MAS_MODEL_WORKER; stub agents ignore it, the llm agent uses ctx.model
    agent = _agent(args.agent, stub_sleep=args.stub_sleep, provider=provider)
    exec_factory = _exec_backend_factory(args.exec_backend) if args.agent == "llm" else None
    ws = _workspace(args.workspace)
    print(
        f"  workspace={ws.name}"
        + (f"  repos={settings().repo_root}  worktrees={settings().worktree_root}" if ws.name == "git" else "")
    )
    workers = [
        Worker(
            f"worker-{i + 1}",
            sorted(caps),
            agent,
            poll_s=0.1,
            run_id=run.id,
            workspace=ws,
            provider=provider,
            exec_backend_factory=exec_factory,
        )
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

    if args.stub_verifier or args.verifier_fail or args.verifier_fail_times:
        verifier = StubVerifier(passed=not args.verifier_fail, fail_times=args.verifier_fail_times)
    else:
        verifier = _acceptance_verifier()
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
        scheduler.gc_workspace(run.id, ws)  # workers are gone: nothing of this run stays on disk but its bare repo
    elapsed = time.monotonic() - t0
    m = metrics.compute(conn, run.id)
    reason = f"  reason={final.verdict_reason}" if final.verdict_reason else ""
    print(f"\n{final.status.value}  verdict={final.verdict}{reason}  in {elapsed:.2f}s")
    _print_metrics(m)
    for w in workers:
        s = w.stats
        print(
            f"  {w.worker_id}: claimed={s.claimed} completed={s.completed} failed={s.failed} "
            f"stale={s.stale} died={s.died}  tasks={s.tasks}"
        )
    conn.close()
    return 0 if final.status.value == "PASSED" else 1


def _print_waiting(conn, run_id) -> None:
    """What a run parked in AWAITING_INPUT is waiting for: the planner's questions, or a contract proposal to approve."""
    from mas.planner import contracts as contract_mod

    qs = runs_mod.pending_questions(conn, run_id)
    if qs:
        print("  AWAITING_INPUT - the planner asked:")
        for i, q in enumerate(qs, 1):
            print(f"    Q{i}: {q}")
        print(f'  -> mas answer {run_id} "your answer"')
        return
    prop = contract_mod.pending_proposal(conn, run_id)
    if prop and runs_mod.sm.get_run(conn, run_id).status.value == "AWAITING_INPUT":
        p = prop["proposal"]
        print("  AWAITING_INPUT - the planner proposes this acceptance contract (definition of done):")
        for r in p.get("requirements", []):
            print(f"    - {r}")
        print(f"    checks: {[c.get('id') + ':' + c.get('type', '') for c in p['contract'].get('checks', [])]}")
        if p.get("assumptions"):
            print(f"    assumptions: {p['assumptions']}")
        if p.get("exclusions"):
            print(f"    exclusions: {p['exclusions']}")
        print(f"  -> mas approve {run_id}            (or: mas approve {run_id} --contract edited.json)")


def _print_metrics(m: metrics.RunMetrics) -> None:
    print(
        f"  tasks={m.tasks} {m.tasks_by_status}\n"
        f"  attempts={m.attempts} {m.attempts_by_status}  retries={m.retries} abandoned={m.abandoned} timeouts={m.timeouts}\n"
        f"  wall_clock={m.wall_clock_s}s  sum_attempt={m.sum_attempt_s}s  "
        f"max_concurrent={m.max_concurrent_attempts}  parallelism_eff={m.parallelism_efficiency}\n"
        f"  total={m.total_s}s  machine={m.machine_s}s  human_wait={m.human_wait_s}s  questions={m.questions}"
        f"  replans={m.replans_used}\n"
        f"  tokens in/out={m.input_tokens}/{m.output_tokens} cost=${m.cost_usd}  events={m.events}\n"
        f"  budget used: tokens={m.tokens_used}/{m.max_tokens} (attempts + planner)  cost=${m.cost_used_usd}/{m.max_cost_usd}"
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
    """Create a run and exit; the compose `orchestrator` + `worker` services execute it. With --goal and no --dag the
    orchestrator's planner (`mas orchestrate --planner llm|fake`) plans it: contract proposal -> `mas approve` -> DAG."""
    if not args.dag and not args.goal:
        raise SystemExit("mas submit needs --dag FILE or --goal TEXT")
    config = normalize_config(args.config)
    dag = DagSpec.from_file(args.dag) if args.dag else None
    if dag is not None:
        if args.benchmark:
            dag.benchmark = args.benchmark
        dag = dag_for_config(dag, config)
    elif config in {"A", "B"}:
        if not args.goal:
            raise SystemExit("configs A/B need --goal or --dag")
        try:
            dag = single_agent_dag(args.goal, args.benchmark)
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc
    max_concurrency = effective_concurrency(config, args.max_concurrency)
    budgets = Budgets(
        max_concurrency=max_concurrency,
        lease_s=args.lease_s,
        max_wallclock_s=args.max_wallclock_s,
        max_attempts_per_task=args.max_attempts,
        max_attempt_runtime_s=args.max_attempt_runtime_s,
        max_tokens=args.max_tokens,
        max_attempt_tokens=args.max_attempt_tokens,
        max_replans=args.max_replans,
        max_cost_usd=args.max_cost_usd,
    )
    conn = connect()
    migrate(conn)
    if dag is None:
        run = runs_mod.create_run(
            conn, goal=args.goal, budgets=budgets, benchmark=args.benchmark, config=config, pool=args.pool
        )
        print(f"submitted run {run.id} (goal, to be planned) status={run.status.value} pool={args.pool}")
    else:
        run = runs_mod.create_run_from_dag(
            conn,
            dag,
            goal=args.goal,
            budgets=budgets,
            benchmark=args.benchmark,
            config=config,
            capabilities=set(settings().worker_capabilities),
            pool=args.pool,
        )
        print(f"submitted run {run.id} ({len(dag.tasks)} tasks) status={run.status.value} pool={args.pool}")
    if not args.wait:
        conn.close()
        return 0
    t0 = time.monotonic()
    shown = None
    while True:
        cur = runs_mod.sm.get_run(conn, run.id)
        if cur.status.terminal:
            break
        if cur.status.value == "AWAITING_INPUT" and shown != cur.questions_asked:
            shown = cur.questions_asked
            _print_waiting(conn, run.id)
        if time.monotonic() - t0 > args.timeout:
            print(f"timeout: run still {cur.status.value}")
            conn.close()
            return 3
        time.sleep(0.5)
    m = metrics.compute(conn, run.id)
    print(f"{cur.status.value}  verdict={cur.verdict}" + (f"  reason={cur.verdict_reason}" if cur.verdict_reason else ""))
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
    planner = _planner(args.planner, spec=args.planner_model)
    caps = set(settings().worker_capabilities)
    if args.run:
        final = scheduler.run_until_terminal(
            conn, UUID(args.run), verifier=verifier, tick_s=args.tick_s, planner=planner, capabilities=caps
        )
        rs = f" reason={final.verdict_reason}" if final.verdict_reason else ""
        print(f"{final.status.value} verdict={final.verdict}{rs}")
        return 0 if final.status.value == "PASSED" else 1
    pools = _pools(args.pool)
    ws = _workspace(None)
    conn.close()
    print(
        f"orchestrator: watching open runs in pools={pools} workspace={ws.name} verifier={kind} "
        f"planner={planner.name if planner else 'none'} parallel={args.parallel} (Ctrl-C to stop)"
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
            planner=planner,
            capabilities=caps,
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
            SELECT ar.type, ar.status, ar.ref, ar.meta, ar.superseded_by, t.key, a.attempt_number, ar.created_at
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
        sup = f"  superseded_by={str(r['superseded_by'])[:8]}" if r["superseded_by"] else ""
        print(f"  {r['type']:14s} {r['status']:10s} {who:12s} {r['ref']}{sup}")
        if r["type"] == "decision":
            m = r["meta"] or {}
            losers = [str(x)[:8] for x in m.get("losers") or []]
            print(f"      winner={str(m.get('winner'))[:8]} losers={losers}  {m.get('rationale', '')}")
    return 0


def cmd_contract(args: argparse.Namespace) -> int:
    """Validate an ADR-007 acceptance contract with the trusted adapter schema; print its check ids and, if the
    contract sits inside an acceptance suite dir, the suite digest a freeze would pin. Unmappable -> exit 2."""
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
    """--pool 'a,b' -> ['a','b']; --pool '*' -> None (serve every pool); default -> [MAS_POOL]."""
    raw = arg if arg is not None else settings().pool
    if raw.strip() == "*":
        return None
    return [p.strip() for p in raw.split(",") if p.strip()]


def _planner(kind: str | None, *, spec: str | None = None):
    """--planner llm (provider from --planner-model or $MAS_MODEL_PLANNER) | fake (offline demo double `fake:planner`)
    | none. The planner proposes; runs.plan_run decides (validator, contract gate, budgets)."""
    kind = (kind or "none").strip().lower()
    if kind in ("none", "", "off"):
        return None
    from mas import providers
    from mas.db import connect as _connect
    from mas.planner.llm import LLMPlanner
    from mas.providers.telemetry import DbSink

    if kind == "fake":
        provider = providers.from_spec("fake:planner")
    elif kind == "llm":
        spec = spec if spec is not None else settings().model_planner
        if not spec:
            raise SystemExit("--planner llm needs a model: pass --planner-model <provider>:<model> or set MAS_MODEL_PLANNER")
        provider = providers.from_spec(spec)
    else:
        raise SystemExit(f"unknown planner {kind!r} (llm | fake | none)")
    tconn = _connect()  # planner telemetry connection (role=planner rows in model_calls)
    return LLMPlanner(provider, sink_factory=lambda run_id: DbSink(tconn), pricing=providers.pricing_from_settings())


def _worker_provider(spec: str | None):
    """--model <provider>:<model> or $MAS_MODEL_WORKER; None = no model (stub agents). Only mas/providers names vendors."""
    from mas import providers

    spec = spec if spec is not None else settings().model_worker
    return providers.from_spec(spec) if spec else None


def _agent(kind: str, *, stub_sleep: float, provider) -> Any:
    """--agent stub (default; no model needed) | llm (needs a model: bounded tool-call loop, mas/workers/llm.py)."""
    if kind == "stub":
        return StubAgent(default_script={"sleep_s": stub_sleep})
    if kind == "llm":
        if provider is None:
            raise SystemExit("--agent llm needs a model: pass --model <provider>:<model> or set MAS_MODEL_WORKER")
        from mas.workers.llm import LLMAgent

        return LLMAgent()
    raise SystemExit(f"unknown agent {kind!r}")


def _exec_backend_factory(kind: str | None, *, worker_id: str = "worker"):
    """MAS_EXEC_BACKEND / --exec-backend:
    sandbox (default; one hardened container per attempt, needs Docker on this host) |
    remote  (compose workers: no docker.sock; commands go through Postgres to `mas execute --watch` on a Docker host) |
    none.   Never 'local' — that backend is test-only. Without Docker the sandbox is unavailable -> no command tools."""
    from mas.workers.execution import SandboxExecutionBackend, sandbox_spec_from_settings

    kind = (kind or settings().exec_backend).strip().lower()
    if kind in ("none", "off", ""):
        return None
    if kind == "remote":
        from mas.workers.exec_remote import RemoteExecutionBackend

        db_url = settings().database_url
        return lambda worktree, claim: RemoteExecutionBackend(
            db_url, run_id=claim.run.id, task_id=claim.task.id, attempt_id=claim.attempt.id, worker_id=worker_id
        )
    if kind != "sandbox":
        raise SystemExit(f"unknown execution backend {kind!r} (sandbox | remote | none)")
    if shutil.which(settings().exec_docker or "docker") is None:
        print("warning: docker not found; command tools disabled for this worker (MAS_EXEC_BACKEND=sandbox)", file=sys.stderr)
        return None
    spec = sandbox_spec_from_settings()
    return lambda worktree, claim: SandboxExecutionBackend(worktree, attempt_id=claim.attempt.id, spec=spec)


def cmd_execute(args: argparse.Namespace) -> int:
    """Execution-runner service: claims workers' command requests (exec_requests) and runs them in per-attempt sandbox
    containers on this host. Runs where Docker is — typically the host, next to `mas verify --watch`."""
    from mas.workers.exec_runner import ExecRunner
    from mas.workers.execution import sandbox_spec_from_settings

    if shutil.which(settings().exec_docker or "docker") is None:
        print("docker not found: the execution runner needs Docker on this host", file=sys.stderr)
        return 2
    conn = connect()
    migrate(conn)
    conn.close()
    runner = ExecRunner(
        settings().database_url,
        worktree_root=Path(settings().worktree_root),
        spec=sandbox_spec_from_settings(),
        max_parallel=args.parallel,
        tick_s=args.tick_s,
        runner_id=args.id,
    )
    if args.once:
        conn = connect()
        try:
            n = runner.run_once(conn)
        finally:
            runner.close_all()
            conn.close()
        print(f"executed {n} request(s)")
        return 0
    print(
        f"execution runner {runner.runner_id}: worktrees={runner.worktree_root} image={runner.spec.image} "
        f"parallel={args.parallel} (Ctrl-C to stop)"
    )
    stop = threading.Event()
    try:
        runner.serve_forever(stop)
    except KeyboardInterrupt:
        stop.set()
    return 0


def cmd_worker(args: argparse.Namespace) -> int:
    kind = "stub" if args.stub and args.agent is None else (args.agent or "stub")
    caps = [c.strip() for c in (args.capabilities or ",".join(settings().worker_capabilities)).split(",") if c.strip()]
    wid = args.id or f"worker-{socket.gethostname()}-{os.getpid()}"
    pools = _pools(args.pool)
    ws = _workspace(args.workspace)
    provider = _worker_provider(args.model)
    w = Worker(
        wid,
        caps,
        _agent(kind, stub_sleep=args.stub_sleep, provider=provider),
        poll_s=settings().worker_poll_s,
        pools=pools,
        workspace=ws,
        provider=provider,
        exec_backend_factory=_exec_backend_factory(args.exec_backend, worker_id=wid) if kind == "llm" else None,
    )
    model = f"{provider.name}:{provider.model}" if provider else "none"
    print(f"{wid}: agent={kind} capabilities={caps} pools={pools} model={model} (Ctrl-C to stop)")
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
    ceiling = cfg.attempt_max_tokens if cfg.attempt_max_tokens is not None else "(run's max_attempt_tokens)"
    print(f"  attempt budget: max_calls={cfg.attempt_max_calls} max_tokens={ceiling}")
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


def cmd_gateway(args: argparse.Namespace) -> int:
    """Model gateway: the one process that holds a vendor key. Listens on the backend network; workers use the openai:
    provider pointed at it. Upstream = MAS_GATEWAY_UPSTREAM (<provider>:<model>), allow-list = MAS_GATEWAY_MODELS."""
    from mas import providers
    from mas.providers.gateway import ModelGateway

    cfg = settings()
    spec = args.upstream or cfg.gateway_upstream
    if not spec:
        print("gateway: set MAS_GATEWAY_UPSTREAM=<provider>:<model> (or --upstream)", file=sys.stderr)
        return 2
    host, _, port = (args.listen or cfg.gateway_listen).rpartition(":")
    upstream = providers.from_spec(spec, cfg=cfg)
    gw = ModelGateway(
        upstream,
        allowed_models=[m for m in (args.models or cfg.gateway_models).split(",") if m.strip()],
        token=args.token or cfg.gateway_token or None,
        max_body_bytes=cfg.gateway_max_body,
        timeout_s=cfg.provider_timeout_s,
        listen=(host or "0.0.0.0", int(port or 8080)),
    )
    print(
        f"gateway: listening on {gw.address[0]}:{gw.address[1]} upstream={upstream.name}:{upstream.model} "
        f"models={sorted(gw.allowed_models)} auth={'token' if gw.token else 'none'} (Ctrl-C to stop)"
    )
    stop = threading.Event()
    try:
        gw.serve_forever(stop)
    except KeyboardInterrupt:
        stop.set()
    return 0


def _result_record(conn, run_id: UUID) -> dict[str, Any]:
    run = runs_mod.sm.get_run(conn, run_id)
    if run.status.value != "PASSED":
        raise ValueError(f"run is {run.status.value}, not PASSED; only externally verified results can be exported")
    integration = conn.execute(
        """
        SELECT ar.id, ar.ref, ar.meta, t.key
        FROM artifacts ar JOIN tasks t ON t.id = ar.task_id
        WHERE ar.run_id = %s AND ar.type = 'git_commit' AND ar.status = 'accepted' AND t.capability = 'integration'
        ORDER BY ar.created_at DESC LIMIT 1
        """,
        (run_id,),
    ).fetchone()
    if integration is None:
        raise ValueError("PASSED run has no accepted integration artifact")
    verification = conn.execute(
        "SELECT meta FROM artifacts WHERE run_id = %s AND type = 'verification' ORDER BY created_at DESC LIMIT 1",
        (run_id,),
    ).fetchone()
    return {
        "run_id": str(run.id),
        "goal": run.goal,
        "benchmark": run.benchmark,
        "config": run.config,
        "status": run.status.value,
        "verdict": run.verdict,
        "integration_sha": integration["ref"],
        "integration_task": integration["key"],
        "artifact_id": str(integration["id"]),
        "verification": (verification or {}).get("meta", {}),
        "metrics": metrics.compute(conn, run_id).as_dict(),
    }


def cmd_result(args: argparse.Namespace) -> int:
    run_id = UUID(args.run_id)
    try:
        with connect() as conn:
            record = _result_record(conn, run_id)
    except (LookupError, ValueError) as exc:
        print(f"cannot export result: {exc}", file=sys.stderr)
        return 2
    cfg = settings()
    repo = Path(cfg.repo_root).resolve() / f"{run_id}.git"
    record["repository"] = str(repo)
    if not repo.joinpath("HEAD").exists():
        print(f"cannot export result: run repository is missing: {repo}", file=sys.stderr)
        return 2
    if not args.output:
        print(json.dumps(record, indent=2, default=str))
        print(f"export with: mas result {run_id} --output <directory>", file=sys.stderr)
        return 0
    dest = Path(args.output).resolve()
    sidecar = Path(str(dest) + ".mas-result.json")
    if dest.exists() or sidecar.exists():
        print(f"refusing to overwrite existing result path: {dest} or {sidecar}", file=sys.stderr)
        return 2
    dest.parent.mkdir(parents=True, exist_ok=True)
    made = False
    try:
        clone = subprocess.run(["git", "clone", "-q", "--no-checkout", str(repo), str(dest)], capture_output=True, text=True)
        if clone.returncode != 0:
            raise RuntimeError(clone.stderr.strip() or "git clone failed")
        made = True
        checkout = subprocess.run(
            ["git", "-C", str(dest), "checkout", "-q", "-B", "verified-result", record["integration_sha"]],
            capture_output=True,
            text=True,
        )
        if checkout.returncode != 0:
            raise RuntimeError(checkout.stderr.strip() or "git checkout failed")
        sidecar.write_text(json.dumps(record, indent=2, default=str) + "\n", encoding="utf-8")
    except (OSError, RuntimeError) as exc:
        if made:
            shutil.rmtree(dest, ignore_errors=True)
        print(f"cannot export result: {exc}", file=sys.stderr)
        return 2
    print(f"verified repository: {dest}")
    print(f"exact commit:        {record['integration_sha']}")
    print(f"verification record: {sidecar}")
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
        print(f"{m.status}  verdict={m.verdict}" + (f"  reason={m.verdict_reason}" if m.verdict_reason else ""))
        _print_metrics(m)
        if report:
            if m.status == "AWAITING_INPUT":
                with connect() as conn2:
                    _print_waiting(conn2, UUID(args.run_id))
            else:
                print(f"  open: {json.dumps(report, default=str)}")
    return 0


def cmd_approve(args: argparse.Namespace) -> int:
    """Approve (optionally edited) the planner's acceptance-contract proposal: freezes it as the run's definition of done
    (ADR-007) — suite written under acceptance/, immutable artifact, run back to PLANNING."""
    from mas.planner import contracts as contract_mod

    doc = None
    if args.contract:
        doc = json.loads(Path(args.contract).read_text(encoding="utf-8"))
    with connect() as conn:
        try:
            frozen = contract_mod.approve(
                conn, UUID(args.run_id), acceptance_root=Path(settings().acceptance_root), contract_doc=doc, approved_by=args.by
            )
        except (contract_mod.InvalidProposal, runs_mod.sm.IllegalTransition) as e:
            print(f"cannot approve: {e}", file=sys.stderr)
            return 2
    print(f"contract frozen for run {args.run_id}: benchmark={frozen.benchmark} sha256={frozen.sha256[:12]}")
    print(f"  suite: {frozen.suite_dir}")
    print("  run is PLANNING again; the planner will now produce the DAG")
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

    dr = sub.add_parser("doctor", help="preflight Docker, Postgres, verifier image, workspaces and model configuration")
    dr.add_argument("--require-live", action="store_true", help="also require a non-fake upstream, role models, prices and key")
    dr.add_argument("--json", action="store_true")
    dr.set_defaults(fn=cmd_doctor)

    up = sub.add_parser("up", help="start Compose and supervise the trusted host executor + verifier in this terminal")
    up.add_argument("--workers", type=int, default=3)
    up.add_argument("--offline", action="store_true", help="use fake:builder/fake planner to prove plumbing without a key")
    up.add_argument("--build", action="store_true", help="build the app and verifier images first")
    up.add_argument("--down-on-exit", action="store_true", help="also stop Compose containers on Ctrl-C")
    up.set_defaults(fn=cmd_up)

    dn = sub.add_parser("down", help="stop Compose services; database and run data are retained by default")
    dn.add_argument("--volumes", action="store_true", help="also delete the Postgres volume (destructive)")
    dn.set_defaults(fn=cmd_down)

    r = sub.add_parser("run", help="in-process run with stub workers")
    r.add_argument("--dag", default=None, help="hand-written DAG file (or use --goal with --planner)")
    r.add_argument("--goal", default=None)
    r.add_argument("--benchmark", default=None)
    r.add_argument("--config", default="D", type=str.upper, choices=CONFIGS, help="frozen A|B|C|D policy (evaluation.md)")
    r.add_argument("--workers", type=int, default=3)
    r.add_argument("--max-concurrency", type=int, default=4)
    r.add_argument("--max-attempts", type=int, default=3)
    r.add_argument("--lease-s", type=int, default=5)
    r.add_argument("--max-wallclock-s", type=int, default=300, help="run budget; the run ABORTS with a verdict when exceeded")
    r.add_argument("--max-attempt-runtime-s", type=int, default=120)
    r.add_argument(
        "--max-replans", type=int, default=1, help="bounded repair cycles after a verifier FAIL (13-lite; the only repair budget)"
    )
    r.add_argument("--max-tokens", type=int, default=2_000_000, help="run token budget")
    r.add_argument("--max-cost-usd", type=float, default=20.0, help="run cost budget (USD, priced from MAS_MODEL_PRICES)")
    r.add_argument(
        "--max-attempt-tokens",
        type=int,
        default=200_000,
        help="per-attempt token allocation; validator rule 8 requires max_tokens to fund one attempt per open task",
    )
    r.add_argument("--stub-sleep", type=float, default=0.5, help="simulated work per attempt (s)")
    r.add_argument("--agent", default="stub", choices=["stub", "llm"], help="stub (no model) | llm (bounded tool-call loop)")
    r.add_argument("--planner", default=None, help="llm | fake — plans an ad-hoc --goal (contract -> approve -> DAG)")
    r.add_argument("--planner-model", default=None, help="<provider>:<model> for --planner llm (default: $MAS_MODEL_PLANNER)")
    r.add_argument("--model", default=None, help="<provider>:<model> for --agent llm (default: $MAS_MODEL_WORKER)")
    r.add_argument("--exec-backend", default=None, help="sandbox (default, needs Docker) | none — command tools for llm agents")
    r.add_argument("--chaos-kill-after", type=float, default=None, help="kill a busy worker after N seconds (A5 demo)")
    r.add_argument("--verifier-fail", action="store_true", help="stub verifier returns FAIL")
    r.add_argument(
        "--verifier-fail-times",
        type=int,
        default=0,
        help="stub verifier FAILs this many times, then passes (bounded-repair demo: FAIL -> repair -> PASS)",
    )
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

    rs = sub.add_parser("result", help="show or export the exact externally verified repository commit")
    rs.add_argument("run_id")
    rs.add_argument("--output", default=None, help="new directory for a normal Git checkout; never overwritten")
    rs.set_defaults(fn=cmd_result)

    ap = sub.add_parser("approve", help="approve the planner's acceptance-contract proposal (freezes the definition of done)")
    ap.add_argument("run_id")
    ap.add_argument("--contract", default=None, help="edited contract JSON to freeze instead of the proposal as-is")
    ap.add_argument("--by", default="human")
    ap.set_defaults(fn=cmd_approve)

    an = sub.add_parser("answer", help="answer a run's pending clarifying questions (ADR-006)")
    an.add_argument("run_id")
    an.add_argument("text")
    an.add_argument("--by", default="human")
    an.set_defaults(fn=cmd_answer)

    sb = sub.add_parser("submit", help="create a run from a DAG file and exit (services pick it up)")
    sb.add_argument("--dag", default=None, help="DAG file; omit to submit an ad-hoc --goal for the orchestrator planner")
    sb.add_argument("--goal", default=None)
    sb.add_argument("--benchmark", default=None)
    sb.add_argument("--config", default="D", type=str.upper, choices=CONFIGS)
    sb.add_argument("--max-concurrency", type=int, default=4)
    sb.add_argument("--max-attempts", type=int, default=3)
    sb.add_argument("--lease-s", type=int, default=15)
    sb.add_argument("--max-wallclock-s", type=int, default=600)
    sb.add_argument("--max-attempt-runtime-s", type=int, default=120)
    sb.add_argument("--max-replans", type=int, default=1)
    sb.add_argument("--max-tokens", type=int, default=2_000_000)
    sb.add_argument("--max-cost-usd", type=float, default=20.0)
    sb.add_argument("--max-attempt-tokens", type=int, default=200_000)
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
    o.add_argument("--planner", default=None, help="llm | fake — plan ad-hoc goals submitted with `mas submit --goal`")
    o.add_argument("--planner-model", default=None, help="<provider>:<model> for --planner llm (default: $MAS_MODEL_PLANNER)")
    o.set_defaults(fn=cmd_orchestrate)

    vf = sub.add_parser("verify", help="verifier service: real sandboxed verdicts for runs left in VERIFYING")
    vf.add_argument("--watch", action="store_true")
    vf.add_argument("--once", action="store_true", help="verify all currently VERIFYING runs once and exit")
    vf.add_argument("--tick-s", type=float, default=settings().orchestrator_tick_s)
    vf.add_argument("--pool", default=None, help="comma-separated pools to serve; '*' = all (default: $MAS_POOL or 'default')")
    vf.add_argument("--parallel", type=int, default=2, help="max concurrent verifications (each is a sandbox)")
    vf.add_argument("--stub-verifier", action="store_true", help="explicit test mode only")
    vf.set_defaults(fn=cmd_verify)

    ex = sub.add_parser("execute", help="execution-runner service: run workers' command requests in per-attempt sandboxes")
    ex.add_argument("--watch", action="store_true")
    ex.add_argument("--once", action="store_true", help="execute all currently pending requests once and exit")
    ex.add_argument("--tick-s", type=float, default=0.2)
    ex.add_argument("--parallel", type=int, default=4, help="max concurrent commands (each in its attempt's sandbox)")
    ex.add_argument("--id", default=None, help="runner id (default: host-derived)")
    ex.set_defaults(fn=cmd_execute)

    w = sub.add_parser("worker", help="worker service")
    w.add_argument("--stub", action="store_true", help="alias for --agent stub")
    w.add_argument(
        "--agent", default=None, choices=["stub", "llm"], help="stub (default, no model) | llm (bounded tool-call loop)"
    )
    w.add_argument("--exec-backend", default=None, help="sandbox (default, needs Docker) | none — command tools for llm agents")
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

    gw = sub.add_parser("gateway", help="model gateway: the one process with a vendor key; workers use openai:<name> at its URL")
    gw.add_argument("--upstream", default=None, help="<provider>:<model> (default: $MAS_GATEWAY_UPSTREAM)")
    gw.add_argument("--models", default=None, help="comma-separated names clients may request (default: the upstream model)")
    gw.add_argument("--token", default=None, help="bearer token clients must present (default: $MAS_GATEWAY_TOKEN)")
    gw.add_argument("--listen", default=None, help="host:port (default: $MAS_GATEWAY_LISTEN or 0.0.0.0:8080)")
    gw.set_defaults(fn=cmd_gateway)

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
