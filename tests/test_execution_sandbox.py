"""Step 10, part 1 — SandboxExecutionBackend: model-authored commands run in a per-attempt container that sees exactly
the attempt's worktree and nothing else. Docker tests (skipped without a daemon); the image is the verifier image."""

from __future__ import annotations

import subprocess
import threading
import time
from pathlib import Path

import pytest

from mas.workers.execution import ExecutionError, SandboxExecutionBackend, SandboxSpec
from mas.workers.tools import ToolLayer, ToolLimits

pytestmark = pytest.mark.docker


@pytest.fixture
def wt(tmp_path: Path) -> Path:
    r = tmp_path / "attempt-a"
    (r / "src").mkdir(parents=True)
    (r / "src" / "app.py").write_text("print('hi')\n", encoding="utf-8")
    (r / "test_app.py").write_text("def test_ok():\n    assert 1 + 1 == 2\n", encoding="utf-8")
    other = tmp_path / "attempt-b"
    other.mkdir()
    (other / "secret.txt").write_text("other attempt's file", encoding="utf-8")
    (tmp_path / "host-secret.txt").write_text("host file", encoding="utf-8")
    return r


def _containers(name: str) -> str:
    return subprocess.run(["docker", "ps", "-a", "-q", "--filter", f"name={name}"], capture_output=True, text=True).stdout.strip()


def test_sandbox_confines_to_the_worktree(wt: Path, tmp_path: Path, verifier_image: str):
    spec = SandboxSpec(image=verifier_image, max_life_s=120)
    with SandboxExecutionBackend(wt, attempt_id="deadbeef-1234", spec=spec) as sb:
        assert sb.confined and sb.container.startswith("mas-exec-deadbeef1234")
        r = sb.run_shell("pwd; ls; cat src/app.py", timeout_s=30)
        assert r.exit_code == 0 and r.output.splitlines()[0] == "/work" and "print('hi')" in r.output, r.output
        # nothing of the host: the sibling attempt and the host file do not exist inside; /data is not mounted
        r = sb.run_shell(
            f"ls {tmp_path.as_posix()} 2>&1; ls /data 2>&1; ls /work/../attempt-b 2>&1; cat /etc/hostname",
            timeout_s=30,
        )
        assert "other attempt" not in r.output and "host file" not in r.output and "secret.txt" not in r.output
        assert "No such file" in r.output
        # writes land in the worktree and are visible to the host (the runtime commits them later)
        r = sb.run_shell("echo made-inside > src/new.txt && cat src/new.txt", timeout_s=30)
        assert r.exit_code == 0 and (wt / "src" / "new.txt").read_text().strip() == "made-inside"
        # network none
        r = sb.run(
            ["python", "-c", "import socket; socket.create_connection(('1.1.1.1', 53), timeout=3); print('CONNECTED')"],
            timeout_s=30,
        )
        assert r.exit_code != 0 and "CONNECTED" not in r.output
        # read-only rootfs, non-root, tmpfs /tmp is writable and private to the container
        r = sb.run_shell("id -u; touch /rootfile 2>&1; echo t > /tmp/f && cat /tmp/f", timeout_s=30)
        lines = r.output.splitlines()
        assert lines[0] != "0" and "Read-only file system" in r.output and lines[-1] == "t"
        assert not (wt / "f").exists()
        assert sb.alive() and sb.commands == 5
    assert _containers(sb.container) == ""  # close() removed it


def test_sandbox_timeout_cancel_and_descendants(wt: Path, verifier_image: str):
    spec = SandboxSpec(image=verifier_image, max_life_s=120, max_output_bytes=5_000)
    with SandboxExecutionBackend(wt, spec=spec) as sb:
        t0 = time.monotonic()
        r = sb.run_shell("sleep 30; echo done", timeout_s=1)
        assert r.timed_out and "done" not in r.output and time.monotonic() - t0 < 12, r
        # a background descendant left by a *normal* exit dies with the sandbox, never with the host
        r = sb.run_shell("sh -c 'sleep 300' & echo started", timeout_s=10)
        assert r.exit_code == 0 and "started" in r.output
        # slim image: no ps/pgrep — count exact ['sleep', '300'] argv vectors in /proc (the probe itself never matches)
        count = [
            "python",
            "-c",
            "import glob; print(sum(1 for f in glob.glob('/proc/[0-9]*/cmdline') "
            "if open(f, 'rb').read().split(bytes([0]))[:2] == [b'sleep', b'300']))",
        ]
        r = sb.run(count, timeout_s=10)
        assert r.exit_code == 0 and int(r.output.strip().splitlines()[-1]) >= 1, r.output
        # output flood is cut and the sandbox is reset (fresh container for the next command)
        r = sb.run(["python", "-c", "import sys\nwhile True: sys.stdout.write('x' * 65536); sys.stdout.flush()"], timeout_s=20)
        assert r.truncated and len(r.output) < 6_000
        r = sb.run(count, timeout_s=10)
        assert r.output.strip().splitlines()[-1] == "0", r.output  # the flood reset the container: the 300 s sleeper is gone
        # cancel mid-command
        cancel = threading.Event()
        threading.Timer(0.5, cancel.set).start()
        t0 = time.monotonic()
        r = sb.run_shell("sleep 30", timeout_s=20, cancel=cancel)
        assert r.cancelled and time.monotonic() - t0 < 12
    assert _containers(sb.container) == ""


def test_tool_layer_with_sandbox_end_to_end(wt: Path, verifier_image: str):
    spec = SandboxSpec(image=verifier_image, max_life_s=120)
    sb = SandboxExecutionBackend(wt, attempt_id="e2e", spec=spec)
    with ToolLayer(wt, ["filesystem", "python", "shell"], backend=sb, limits=ToolLimits(command_timeout_s=60)) as tl:
        assert "run_pytest" in tl.tool_names()
        # the jail writes on the host; the sandbox sees the same worktree
        assert not tl.dispatch("write_file", {"path": "test_more.py", "content": "def test_two():\n    assert True\n"}).is_error
        r = tl.dispatch("run_pytest", {})
        assert "exit_code=0" in r.content and "2 passed" in r.content, r.content
        r = tl.dispatch("run_command", {"command": "cat /etc/passwd | head -1; ls /work"})
        assert "root:" in r.content and "test_more.py" in r.content  # its own rootfs, its own worktree
        r = tl.dispatch("run_python", {"code": "open('/work/../escape.txt', 'w')"})
        assert "Read-only file system" in r.content or "exit_code=1" in r.content  # / is read-only; nothing escapes
    assert _containers(sb.container) == ""


def test_sandbox_start_failure_is_an_execution_error(wt: Path):
    sb = SandboxExecutionBackend(wt, spec=SandboxSpec(image="mas-no-such-image:never", docker="docker"))
    with pytest.raises(ExecutionError):
        sb.run_shell("true", timeout_s=5)
    sb.close()
