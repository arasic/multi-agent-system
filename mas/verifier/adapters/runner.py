"""Trusted in-sandbox acceptance runner (ADR-007 §4a). Baked into the verifier image at /opt/mas/adapters/.

    python /opt/mas/adapters/runner.py /acceptance/contract.json

Reads the (already host-validated) contract, re-validates it defensively with the same schema module, copies the
read-only source (/input) into the disposable workspace (/work/repo), executes each typed check with its own
timeout, and prints exactly one protocol-1 JSON report on stdout — the same protocol hand-written suites use, so
the verifier does not know or care that adapters produced it.

Service lifecycle (http_status / restart_persists) is owned here: start with a restricted environment, wait for
GET health == 200, stop with terminate → kill, and always stop in `finally`. {state_dir} survives restarts within
one verification (that is what restart_persists tests); everything is inside the sandbox tmpfs.

Never imports `mas`: the image is python:3.12-slim + pytest; only stdlib and the copied schema.py.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import schema  # noqa: E402  (copied next to this file in the image)

SOURCE = Path(os.environ.get("MAS_ACCEPTANCE_INPUT", "/input"))
WORK = Path(os.environ.get("MAS_ACCEPTANCE_WORK", "/work"))
REPO = WORK / "repo"
STATE = WORK / "state"
OUTPUT_TAIL = 2000


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _env() -> dict[str, str]:
    return {
        "PATH": os.environ.get("PATH", ""),
        "HOME": "/tmp",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONUNBUFFERED": "1",
        "MAS_STATE_DIR": str(STATE),
    }


def _tail(b: bytes) -> str:
    return b.decode("utf-8", errors="replace")[-OUTPUT_TAIL:]


def _run(cmd: list[str], timeout_s: int) -> tuple[int | None, str]:
    """Run a command in the repo with a hard timeout. Returns (exit code or None on timeout, output tail)."""
    try:
        p = subprocess.run(
            cmd, cwd=REPO, env=_env(), stdin=subprocess.DEVNULL, capture_output=True, timeout=timeout_s, check=False
        )
    except subprocess.TimeoutExpired as exc:
        out = (exc.stdout or b"") + b"\n" + (exc.stderr or b"")
        return None, _tail(out)
    except OSError as exc:
        return 127, f"cannot execute {cmd[0]!r}: {exc}"
    return p.returncode, _tail(p.stdout + b"\n" + p.stderr)


# ----------------------------------------------------------------------------- service lifecycle


class Service:
    def __init__(self, spec: schema.Service):
        self.spec = spec
        self.proc: subprocess.Popen | None = None
        self.base = f"http://127.0.0.1:{spec.port}"

    def _cmd(self) -> list[str]:
        subst = {"{port}": str(self.spec.port), "{state_dir}": str(STATE), "{repo}": str(REPO)}
        out = []
        for part in self.spec.start:
            for k, v in subst.items():
                part = part.replace(k, v)
            out.append(part)
        return out

    def start(self) -> None:
        STATE.mkdir(parents=True, exist_ok=True)
        log = (STATE / "service.log").open("ab")
        self.proc = subprocess.Popen(
            self._cmd(), cwd=REPO, env=_env(), stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT
        )
        deadline = time.monotonic() + self.spec.startup_timeout_s
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"service exited during startup with code {self.proc.returncode}")
            try:
                status, _, _ = self.request(schema.HttpRequest("GET", self.spec.health, timeout_s=0.5))
                if status == 200:
                    return
            except Exception:
                pass
            time.sleep(0.05)
        self.stop()
        raise TimeoutError(f"service did not report healthy on GET {self.spec.health} within {self.spec.startup_timeout_s}s")

    def stop(self) -> None:
        p, self.proc = self.proc, None
        if p is None or p.poll() is not None:
            return
        p.terminate()
        try:
            p.wait(timeout=3)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait(timeout=3)

    def request(self, r: schema.HttpRequest) -> tuple[int, dict[str, str], Any]:
        data = None if r.json is None else json.dumps(r.json).encode()
        headers = {"Content-Type": "application/json", **r.headers} if data is not None else dict(r.headers)
        req = urllib.request.Request(self.base + r.path, data=data, method=r.method, headers=headers)
        opener = urllib.request.build_opener(NoRedirect)
        try:
            resp = opener.open(req, timeout=r.timeout_s)
        except urllib.error.HTTPError as exc:
            resp = exc
        raw = resp.read()
        body: Any = None
        if raw:
            try:
                body = json.loads(raw)
            except ValueError:
                body = raw.decode("utf-8", errors="replace")
        return resp.status, {k.lower(): v for k, v in resp.headers.items()}, body


def _check_expect(status: int, headers: dict[str, str], body: Any, expect: schema.HttpExpect) -> str:
    if status != expect.status:
        raise AssertionError(f"expected status {expect.status}, got {status} (body={str(body)[:200]!r})")
    for k, v in expect.header_equals.items():
        if headers.get(k.lower()) != v:
            raise AssertionError(f"expected header {k}={v!r}, got {headers.get(k.lower())!r}")
    if expect.json_contains:
        if not isinstance(body, dict):
            raise AssertionError(f"expected a JSON object body, got {type(body).__name__}")
        for k, v in expect.json_contains.items():
            if k not in body or body[k] != v:
                raise AssertionError(f"expected json[{k!r}] == {v!r}, got {body.get(k, '<missing>')!r}")
    return f"status {status} as expected"


# ----------------------------------------------------------------------------- adapters


def build_succeeds(c: schema.BuildSucceeds, _svc: Service | None) -> str:
    code, out = _run(list(c.command), c.timeout_s)
    if code is None:
        raise TimeoutError(f"build command exceeded {c.timeout_s}s")
    if code != 0:
        raise AssertionError(f"build command exited {code}: {out[-600:]}")
    return "build command exited 0"


_PYTEST_SUMMARY = re.compile(r"(\d+) passed")
_UNITTEST_RAN = re.compile(r"Ran (\d+) tests?")


def tests_required(c: schema.TestsRequired, _svc: Service | None) -> str:
    if c.runner == "pytest":
        cmd = [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", *c.args]
    else:
        cmd = [sys.executable, "-m", "unittest", *c.args]
    code, out = _run(cmd, c.timeout_s)
    if code is None:
        raise TimeoutError(f"test runner exceeded {c.timeout_s}s")
    if c.runner == "pytest":
        if code == 5:
            raise AssertionError("no tests were collected (pytest exit 5)")
        m = _PYTEST_SUMMARY.search(out)
        passed = int(m.group(1)) if m else 0
    else:
        m = _UNITTEST_RAN.search(out)
        passed = int(m.group(1)) if m else 0
    if code != 0:
        raise AssertionError(f"tests failed (exit {code}): {out[-600:]}")
    if passed < c.min_tests:
        raise AssertionError(f"only {passed} test(s) ran; contract requires at least {c.min_tests}")
    return f"{passed} test(s) passed via {c.runner}"


def http_status(c: schema.HttpStatus, svc: Service | None) -> str:
    assert svc is not None
    if svc.proc is None:
        svc.start()
    status, headers, body = svc.request(c.request)
    return _check_expect(status, headers, body, c.expect)


def restart_persists(c: schema.RestartPersists, svc: Service | None) -> str:
    assert svc is not None
    if svc.proc is None:
        svc.start()
    for r in c.setup:
        st, _, body = svc.request(r)
        if st >= 400:
            raise AssertionError(f"setup request {r.method} {r.path} failed with {st}: {str(body)[:200]!r}")
    svc.stop()
    svc.start()
    status, headers, body = svc.request(c.verify)
    _check_expect(status, headers, body, c.expect)
    return "state survived a service restart"


ADAPTERS = {
    "build_succeeds": build_succeeds,
    "tests_required": tests_required,
    "http_status": http_status,
    "restart_persists": restart_persists,
}


# ----------------------------------------------------------------------------- main


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: runner.py <contract.json>", file=sys.stderr)
        return 2
    try:
        contract = schema.parse_contract(json.loads(Path(argv[1]).read_text(encoding="utf-8")))
    except (OSError, ValueError, schema.InvalidContract) as exc:
        # defensive re-validation: an invalid contract must not produce a report the host could mistake for checks
        print(f"invalid contract: {exc}", file=sys.stderr)
        return 3
    REPO.mkdir(parents=True, exist_ok=True)
    shutil.copytree(SOURCE, REPO, dirs_exist_ok=True)
    STATE.mkdir(parents=True, exist_ok=True)

    svc = Service(contract.service) if contract.service else None
    results: list[dict[str, Any]] = []
    try:
        for c in contract.checks:
            fn = ADAPTERS[c.type]
            t0 = time.monotonic()
            try:
                detail = fn(c, svc)  # type: ignore[arg-type]
                results.append({"id": c.id, "status": "PASS", "detail": str(detail)[:4000]})
            except Exception as exc:
                results.append({"id": c.id, "status": "FAIL", "detail": f"{type(exc).__name__}: {exc}"[:4000]})
            results[-1]["duration_ms"] = round((time.monotonic() - t0) * 1000)
    finally:
        if svc is not None:
            svc.stop()
    print(json.dumps({"protocol_version": 1, "checks": results}, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
