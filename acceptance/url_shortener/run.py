"""Human-owned URL-shortener acceptance suite. Emits exactly one JSON report."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

SOURCE = Path("/input")
REPO = Path("/work/repo")
DB = Path("/work/urls.sqlite3")
PORT = 18080
BASE = f"http://127.0.0.1:{PORT}"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def request(path: str, *, method: str = "GET", body=None, timeout: float = 2.0):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BASE + path, data=data, method=method, headers={"Content-Type": "application/json"})
    opener = urllib.request.build_opener(NoRedirect)
    try:
        response = opener.open(req, timeout=timeout)
    except urllib.error.HTTPError as exc:
        response = exc
    raw = response.read()
    payload = json.loads(raw) if raw else None
    return response.status, dict(response.headers), payload


def start():
    app = REPO / "app.py"
    if not app.is_file():
        raise RuntimeError("integration commit has no root app.py")
    env = {"PATH": os.environ.get("PATH", ""), "HOME": "/tmp", "PYTHONDONTWRITEBYTECODE": "1"}
    return subprocess.Popen(
        [sys.executable, str(app), "--port", str(PORT), "--db", str(DB)],
        cwd=REPO,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def stop(proc):
    if proc is None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)


def record(results, check_id, fn):
    t0 = time.monotonic()
    try:
        detail = fn() or "ok"
    except Exception as exc:
        results.append(
            {
                "id": check_id,
                "status": "FAIL",
                "detail": f"{type(exc).__name__}: {exc}",
                "duration_ms": round((time.monotonic() - t0) * 1000),
            }
        )
    else:
        results.append(
            {"id": check_id, "status": "PASS", "detail": str(detail), "duration_ms": round((time.monotonic() - t0) * 1000)}
        )


def main():
    shutil.copytree(SOURCE, REPO, dirs_exist_ok=True)
    results = []
    state = {}
    proc = None

    def service_starts():
        nonlocal proc
        proc = start()
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(f"service exited with code {proc.returncode}")
            try:
                status, _, _ = request("/health", timeout=0.25)
                if status == 200:
                    return "GET /health -> 200"
            except Exception:
                time.sleep(0.05)
        raise TimeoutError("service did not become healthy")

    def shorten():
        status, _, body = request("/shorten", method="POST", body={"url": "https://example.com/a"})
        if status != 201 or not isinstance(body, dict) or not isinstance(body.get("code"), str):
            raise AssertionError(f"expected 201 and string code, got {status} {body!r}")
        state["code"] = body["code"]
        return f"created {body['code']}"

    def resolve():
        code = state.get("code")
        if not code:
            raise AssertionError("shorten check produced no code")
        status, headers, _ = request(f"/{code}")
        if status != 302 or headers.get("Location") != "https://example.com/a":
            raise AssertionError(f"expected redirect, got {status} {headers.get('Location')!r}")
        return "redirect preserved target"

    def stats():
        status, _, body = request("/stats")
        if status != 200 or not isinstance(body, dict) or body.get("urls", 0) < 1:
            raise AssertionError(f"expected persisted count, got {status} {body!r}")
        return "stats reports stored URL"

    def restart():
        nonlocal proc
        code = state.get("code")
        if not code:
            raise AssertionError("shorten check produced no code")
        stop(proc)
        proc = start()
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            try:
                status, headers, _ = request(f"/{code}", timeout=0.25)
                if status == 302 and headers.get("Location") == "https://example.com/a":
                    return "mapping survived process restart"
            except Exception:
                time.sleep(0.05)
        raise AssertionError("mapping did not survive restart")

    try:
        record(results, "service_starts", service_starts)
        record(results, "shorten_returns_201", shorten)
        record(results, "resolve_redirects", resolve)
        record(results, "stats_available", stats)
        record(results, "restart_persists", restart)
    finally:
        stop(proc)
    print(json.dumps({"protocol_version": 1, "checks": results}, separators=(",", ":")))


if __name__ == "__main__":
    main()
