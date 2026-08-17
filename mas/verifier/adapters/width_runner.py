"""Trusted acceptance runner for the M3 width benchmark.

The immutable suite spec names N independent adapters. Candidate code must provide
`adapters/<id>.py: transform(int) -> int`, implement every specified affine mapping, and include a passing pytest suite.
This file is baked into the verifier image; the suite directory only supplies the frozen JSON spec.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

SOURCE = Path(os.environ.get("MAS_ACCEPTANCE_INPUT", "/input"))
REPO = Path(os.environ.get("MAS_ACCEPTANCE_WORK", "/work")) / "repo"


def check(check_id, fn):
    started = time.monotonic()
    try:
        detail = str(fn() or "ok")
        status = "PASS"
    except Exception as exc:  # the report, not a traceback, is the protocol
        detail = f"{type(exc).__name__}: {exc}"[:4000]
        status = "FAIL"
    return {"id": check_id, "status": status, "detail": detail, "duration_ms": round((time.monotonic() - started) * 1000)}


def load_spec(path: Path):
    doc = json.loads(path.read_text(encoding="utf-8"))
    rows = doc.get("adapters") if isinstance(doc, dict) else None
    if not isinstance(rows, list) or not rows:
        raise ValueError("spec.adapters must be a non-empty array")
    for row in rows:
        if set(row) != {"id", "factor", "offset"} or not str(row["id"]).isalnum():
            raise ValueError("invalid adapter spec")
        int(row["factor"])
        int(row["offset"])
    return rows


def files_present(rows):
    missing = [r["id"] for r in rows if not (REPO / "adapters" / f"{r['id']}.py").is_file()]
    if missing:
        raise AssertionError(f"missing adapter modules: {missing}")
    return f"{len(rows)} adapter modules present"


def behavior(rows):
    failures = []
    for row in rows:
        path = REPO / "adapters" / f"{row['id']}.py"
        spec = importlib.util.spec_from_file_location(f"candidate_{row['id']}", path)
        if spec is None or spec.loader is None:
            failures.append(f"{row['id']}: cannot load")
            continue
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        transform = getattr(module, "transform", None)
        if not callable(transform):
            failures.append(f"{row['id']}: no transform")
            continue
        for value in (-17, -1, 0, 2, 19):
            expected = value * int(row["factor"]) + int(row["offset"])
            try:
                actual = transform(value)
            except Exception as exc:
                failures.append(f"{row['id']}({value}) raised {type(exc).__name__}")
                break
            if actual != expected:
                failures.append(f"{row['id']}({value})={actual!r}, expected {expected!r}")
                break
    if failures:
        raise AssertionError("; ".join(failures[:20]))
    return f"{len(rows) * 5} behavior cases passed"


def tests_pass():
    tests = sorted(REPO.glob("test*.py")) + sorted(REPO.glob("tests/test*.py"))
    if not tests:
        raise AssertionError("candidate repository contains no pytest tests")
    env = {"PATH": os.environ.get("PATH", ""), "HOME": "/tmp", "PYTHONDONTWRITEBYTECODE": "1"}
    p = subprocess.run(
        [sys.executable, "-m", "pytest", "-q"], cwd=REPO, env=env, capture_output=True, text=True, timeout=20, check=False
    )
    if p.returncode != 0:
        raise AssertionError((p.stdout + "\n" + p.stderr)[-2000:])
    return p.stdout.strip()[-1000:]


def main():
    rows = load_spec(Path(sys.argv[1]))
    if REPO.exists():
        shutil.rmtree(REPO)
    shutil.copytree(SOURCE, REPO)
    report = {
        "protocol_version": 1,
        "checks": [
            check("adapter_files", lambda: files_present(rows)),
            check("adapter_behavior", lambda: behavior(rows)),
            check("tests_pass", tests_pass),
        ],
    }
    print(json.dumps(report, separators=(",", ":")))


if __name__ == "__main__":
    main()
