"""Fixture tests (pytest) — exist so `tests_required` has something real to count."""

import subprocess
import sys
import time
import urllib.request


def test_health_endpoint_via_real_process(tmp_path):
    proc = subprocess.Popen(
        [sys.executable, "app.py", "--port", "18999", "--db", str(tmp_path / "t.sqlite3")],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                assert urllib.request.urlopen("http://127.0.0.1:18999/health", timeout=0.3).status == 200
                break
            except Exception:
                time.sleep(0.05)
        else:
            raise AssertionError("no health")
    finally:
        proc.terminate()
        proc.wait(timeout=3)


def test_arithmetic_sanity():
    assert 1 + 1 == 2
