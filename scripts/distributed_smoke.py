"""Run one end-to-end Compose/operator smoke and write machine-readable evidence.

Real MVP gate (requires `.env`/shell provider configuration and a clean, dedicated Compose project):

  python scripts/distributed_smoke.py --build --output mvp-evidence/distributed-smoke.json

Key-less rehearsal:

  python scripts/distributed_smoke.py --offline --build --output .t/distributed-offline.json

The script starts `mas up`, waits for Compose, submits the frozen URL-shortener DAG to the default pool, waits for the
real external verifier, exports the exact verified Git result, and shuts down services it started. It refuses to take
over an already-running Compose project (orchestrator / gateway / worker) unless --reuse is explicit; reuse mode never
stops those services. A running `postgres` alone is the documented setup state (`docker compose up -d postgres` ->
`mas doctor` -> `mas up`; the preflight needs it before `mas up` can start it): it is reused and left running.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mas.db import connect  # noqa: E402
from mas.metrics import compute  # noqa: E402

RUN_ID = re.compile(r"\bsubmitted run ([0-9a-f-]{36})\b", re.I)
SERVICES = {"postgres", "orchestrator", "gateway", "worker"}
ACTORS = SERVICES - {"postgres"}  # the services another supervisor may own; postgres is the shared substrate


def blocking_services(existing: set[str]) -> set[str]:
    """Running services that mean another `mas up` may own the project. Postgres alone does not: the preflight
    (`mas doctor`) needs it before `mas up` can start it, and `mas up` re-uses a running one."""
    return set(existing) & ACTORS


def _git_state() -> dict[str, object]:
    rev = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=False)
    dirty = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, capture_output=True, text=True, check=False)
    return {"commit": rev.stdout.strip() if rev.returncode == 0 else "unknown", "dirty": bool(dirty.stdout.strip())}


def running_services() -> set[str]:
    proc = subprocess.run(
        ["docker", "compose", "ps", "--services", "--status", "running"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return {line.strip() for line in proc.stdout.splitlines() if line.strip()} if proc.returncode == 0 else set()


def _write(path: Path, evidence: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _stop_up(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    try:
        if os.name == "nt":
            proc.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
        else:
            proc.send_signal(signal.SIGINT)
        proc.wait(timeout=20)
    except (OSError, subprocess.TimeoutExpired):
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--offline", action="store_true", help="use the fake provider; rehearsal evidence does not complete MVP")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--reuse", action="store_true", help="use already-running services; do not start or stop them")
    ap.add_argument("--startup-timeout", type=float, default=180)
    ap.add_argument("--run-timeout", type=float, default=900)
    ap.add_argument("--output", type=Path, default=ROOT / "mvp-evidence" / "distributed-smoke.json")
    ap.add_argument("--result", type=Path, default=ROOT / "mvp-evidence" / "distributed-result")
    args = ap.parse_args(argv)
    evidence: dict = {
        "schema": 1,
        "started_at": datetime.now(UTC).isoformat(),
        "mode": "offline" if args.offline else "live",
        "git": _git_state(),
        "complete": False,
    }
    _write(args.output, evidence)
    existing = running_services()
    evidence["preexisting_services"] = sorted(existing)
    blocking = blocking_services(existing)
    if blocking and not args.reuse:
        evidence["error"] = f"Compose services already running: {sorted(blocking)} (use --reuse only if supervisors are ready)"
        evidence["finished_at"] = datetime.now(UTC).isoformat()
        _write(args.output, evidence)
        print(evidence["error"], file=sys.stderr)
        return 2

    doctor = [sys.executable, "-m", "mas", "doctor"] + ([] if args.offline else ["--require-live"])
    preflight = subprocess.run(doctor, cwd=ROOT, check=False)
    if preflight.returncode:
        evidence["error"] = f"doctor failed with exit {preflight.returncode}"
        evidence["finished_at"] = datetime.now(UTC).isoformat()
        _write(args.output, evidence)
        return 2

    up: subprocess.Popen | None = None
    try:
        if not args.reuse:
            command = [sys.executable, "-u", "-m", "mas", "up", "--workers", str(args.workers)]
            if args.offline:
                command.append("--offline")
            if args.build:
                command.append("--build")
            flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0  # type: ignore[attr-defined]
            up = subprocess.Popen(command, cwd=ROOT, creationflags=flags)
            deadline = time.monotonic() + args.startup_timeout
            while time.monotonic() < deadline:
                if up.poll() is not None:
                    raise RuntimeError(f"mas up exited during startup with code {up.returncode}")
                if SERVICES.issubset(running_services()):
                    time.sleep(2)  # `mas up` starts the two host supervisors immediately after Compose returns
                    break
                time.sleep(1)
            else:
                raise RuntimeError(f"services did not become ready in {args.startup_timeout:.0f}s")
        elif not SERVICES.issubset(existing):
            raise RuntimeError(f"--reuse requires running services {sorted(SERVICES)}; found {sorted(existing)}")

        submit = subprocess.run(
            [
                sys.executable,
                "-m",
                "mas",
                "submit",
                "--dag",
                "benchmarks/url_shortener/dag.json",
                "--wait",
                "--timeout",
                str(args.run_timeout),
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        print(submit.stdout, end="")
        if submit.stderr:
            print(submit.stderr, file=sys.stderr, end="")
        match = RUN_ID.search(submit.stdout)
        if not match:
            raise RuntimeError(f"submit did not report a run id (exit {submit.returncode})")
        run_id = UUID(match.group(1))
        with connect() as conn:
            measured = compute(conn, run_id)
        priced = measured.unpriced_calls == 0
        evidence["run"] = measured.as_dict()
        evidence["priced"] = priced
        if submit.returncode != 0 or measured.status != "PASSED":
            raise RuntimeError(f"distributed run ended {measured.status} (client exit {submit.returncode})")
        if not args.offline and not priced:
            raise RuntimeError("distributed live run contains unpriced model calls")
        export = subprocess.run(
            [sys.executable, "-m", "mas", "result", str(run_id), "--output", str(args.result)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        print(export.stdout, end="")
        if export.returncode:
            raise RuntimeError(export.stderr.strip() or f"result export failed with exit {export.returncode}")
        evidence["result_path"] = str(args.result.resolve())
        evidence["complete"] = True
        return 0
    except (OSError, RuntimeError) as exc:
        evidence["error"] = str(exc)
        print(f"distributed smoke failed: {exc}", file=sys.stderr)
        return 1
    finally:
        evidence["finished_at"] = datetime.now(UTC).isoformat()
        _write(args.output, evidence)
        if up is not None:
            _stop_up(up)
            if "postgres" in existing:
                # leave the substrate as found: stop and remove only the actors this smoke started
                subprocess.run(["docker", "compose", "rm", "-sf", *sorted(ACTORS)], cwd=ROOT, check=False)
            else:
                subprocess.run([sys.executable, "-m", "mas", "down"], cwd=ROOT, check=False)


if __name__ == "__main__":
    raise SystemExit(main())
