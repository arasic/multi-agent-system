"""Step 10, part 1 — the tool layer with no LLM: path jail, family allow-list, fail-closed command tools, and the
TEST-ONLY local execution backend (bounded: sanitized env, output cap, timeout / deadline / cancel, process-tree kill;
NOT confined — see tests/test_execution_sandbox.py for the confined backend). All offline, no DB."""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from mas.workers.execution import ExecutionUnavailable, LocalExecutionBackend
from mas.workers.tools import (
    SCHEMAS,
    TOOLS_BY_FAMILY,
    Jail,
    PathJailError,
    ToolLayer,
    ToolLimits,
    run_bounded,
    sanitized_env,
)


def local(root: Path, **kw) -> LocalExecutionBackend:
    return LocalExecutionBackend(root, unsafe_ok=True, **kw)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    r = tmp_path / "wt"
    (r / "src").mkdir(parents=True)
    (r / "src" / "app.py").write_text("print('hi')\n", encoding="utf-8")
    (r / ".git").mkdir()
    (r / ".git" / "config").write_text("[core]\n", encoding="utf-8")
    (r / "acceptance").mkdir()
    (r / "acceptance" / "suite.json").write_text("{}", encoding="utf-8")
    (tmp_path / "outside.txt").write_text("secret", encoding="utf-8")
    return r


def _pid_alive(pid: int) -> bool:
    if os.name == "nt":
        out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH"], capture_output=True, text=True, check=False).stdout
        return str(pid) in out
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


# ----------------------------------------------------------------------------- jail


def test_jail_blocks_traversal_absolute_reserved_and_symlink_escape(root: Path, tmp_path: Path):
    j = Jail(root)
    assert j.resolve("src/app.py") == (root / "src" / "app.py").resolve()
    assert j.resolve("./src/./app.py") == (root / "src" / "app.py").resolve()
    assert j.resolve("src\\app.py") == (root / "src" / "app.py").resolve()  # backslashes normalised
    for bad in [
        "../outside.txt",
        "src/../../outside.txt",
        "/etc/passwd",
        "C:/Windows/win.ini",
        "C:\\x",
        "//server/share",
        "",
        "   ",
    ]:
        with pytest.raises(PathJailError):
            j.resolve(bad)
    with pytest.raises(PathJailError, match="reserved"):
        j.resolve(".git/config")
    with pytest.raises(PathJailError):
        j.resolve(".git/../.git/HEAD", for_write=True)
    with pytest.raises(PathJailError, match="reserved"):
        j.resolve(".git/HEAD", for_write=True)
    assert j.resolve("acceptance/suite.json").exists()  # readable...
    with pytest.raises(PathJailError, match="read-only"):
        j.resolve("acceptance/suite.json", for_write=True)  # ...never writable
    # symlink escape (skipped where symlinks need privileges)
    link = root / "src" / "esc"
    try:
        os.symlink(tmp_path / "outside.txt", link)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unavailable")
    with pytest.raises(PathJailError, match="escapes"):
        j.resolve("src/esc")


def test_jail_read_globs_narrow_reads_only(root: Path):
    j = Jail(root, read_globs=["src/**", "README.md"])
    assert j.resolve("src/app.py").exists()
    assert j.resolve("src/deep/x.py", for_write=True)  # writes are governed by the worktree, not the read globs
    with pytest.raises(PathJailError, match="context paths"):
        j.resolve("acceptance/suite.json")
    with pytest.raises(PathJailError, match="context paths"):
        j.resolve("other.txt")


# ----------------------------------------------------------------------------- families / schemas


def test_only_granted_families_are_exposed_and_dispatchable(root: Path):
    with ToolLayer(root, ["filesystem"]) as tl:
        assert tl.tool_names() == ["read_file", "write_file", "list_files"]
        assert [s["name"] for s in tl.schemas()] == tl.tool_names()
        r = tl.dispatch("run_command", {"command": "echo hi"})
        assert r.is_error and "not granted" in r.content
        r = tl.dispatch("nuke_everything", {})
        assert r.is_error and "unknown tool" in r.content
        r = tl.dispatch("read_file", {"nope": 1})
        assert r.is_error and "bad arguments" in r.content
    with ToolLayer(root, ["filesystem", "shell", "python", "git", "model", "network"]) as tl:  # unknown names ignored
        # no execution backend → command tools do not exist for the model (fail closed), even though the families are granted
        assert set(tl.tool_names()) == {t for fam in ("filesystem", "git") for t in TOOLS_BY_FAMILY[fam]}
        r = tl.dispatch("run_command", {"command": "echo hi"})
        assert r.is_error and "confined execution backend" in r.content
    with ToolLayer(root, ["filesystem", "shell", "python", "git", "model", "network"], backend=local(root)) as tl:
        assert set(tl.tool_names()) == {t for fam in ("filesystem", "shell", "python", "git") for t in TOOLS_BY_FAMILY[fam]}
    with pytest.raises(ExecutionUnavailable):  # the unconfined backend must be opted into explicitly; the runtime never does
        LocalExecutionBackend(root)
    assert local(root).confined is False
    assert set(SCHEMAS) == {t for ts in TOOLS_BY_FAMILY.values() for t in ts}
    for s in SCHEMAS.values():
        assert s["input_schema"]["type"] == "object" and s["description"]


# ----------------------------------------------------------------------------- filesystem tools


def test_filesystem_tools_roundtrip_caps_and_denials(root: Path, tmp_path: Path):
    with ToolLayer(root, ["filesystem"], limits=ToolLimits(max_file_bytes=200, max_list_entries=3)) as tl:
        assert tl.dispatch("write_file", {"path": "src/pkg/mod.py", "content": "x = 1\n"}).content.startswith("wrote 6 chars")
        assert tl.dispatch("read_file", {"path": "src/pkg/mod.py"}).content == "x = 1\n"
        listing = tl.dispatch("list_files", {"path": "src", "recursive": True}).content.splitlines()
        assert listing[:3] == ["src/app.py", "src/pkg/", "src/pkg/mod.py"] and listing[-1].startswith("[truncated at 3")
        assert ".git/" not in tl.dispatch("list_files", {}).content
        r = tl.dispatch("write_file", {"path": "big.txt", "content": "y" * 201})
        assert r.is_error and "too large" in r.content
        r = tl.dispatch("write_file", {"path": "acceptance/suite.json", "content": "{}"})
        assert r.is_error and "read-only" in r.content
        r = tl.dispatch("write_file", {"path": "../evil.txt", "content": "z"})
        assert r.is_error and "'..'" in r.content and not (tmp_path / "evil.txt").exists()
        r = tl.dispatch("read_file", {"path": ".git/config"})
        assert r.is_error and "reserved" in r.content
        r = tl.dispatch("read_file", {"path": "src"})
        assert r.is_error and "not a file" in r.content
        assert len(tl.calls) == 9 and tl.calls[0].name == "write_file" and not tl.calls[0].result.is_error


# ----------------------------------------------------------------------------- processes


def test_commands_run_in_root_with_sanitized_env(root: Path, monkeypatch):
    monkeypatch.setenv("FAKE_PROVIDER_KEY", "sk-should-not-leak")
    monkeypatch.setenv("MAS_DATABASE_URL", "postgresql://nope")
    with ToolLayer(root, ["shell", "python"], backend=local(root)) as tl:
        out = tl.dispatch(
            "run_python",
            {
                "code": "import os; print(os.getcwd()); print(os.environ.get('FAKE_PROVIDER_KEY')); "
                "print(os.environ.get('MAS_DATABASE_URL')); print(os.environ['HOME'])"
            },
        )
        assert not out.is_error and out.content.startswith("exit_code=0")
        lines = out.content.splitlines()[1:]
        assert Path(lines[0]).resolve() == root.resolve()
        assert lines[1] == "None" and lines[2] == "None"
        assert Path(lines[3]).resolve() != Path.home().resolve()  # HOME is a throwaway dir, not the worker's
        r = tl.dispatch("run_command", {"command": "echo hello && exit 3"})
        assert "exit_code=3" in r.content and "hello" in r.content
        r = tl.dispatch("run_command", {"command": "   "})
        assert r.is_error
    env = sanitized_env(Path("."))
    assert "FAKE_PROVIDER_KEY" not in env and "MAS_DATABASE_URL" not in env and "PATH" in env


def test_output_cap_kills_the_flood(root: Path):
    with ToolLayer(root, ["python"], backend=local(root, max_output_bytes=10_000), limits=ToolLimits(command_timeout_s=30)) as tl:
        t0 = time.monotonic()
        r = tl.dispatch("run_python", {"code": "import sys\nwhile True: sys.stdout.write('x' * 65536); sys.stdout.flush()"})
        assert time.monotonic() - t0 < 25
        assert "OUTPUT_TRUNCATED" in r.content and "[output truncated]" in r.content
        assert len(r.content) < 10_000 + 200


def test_timeout_kills_the_whole_process_tree(root: Path):
    marker = root / "child.pid"
    code = (
        "import subprocess, sys, time, os\n"
        f"p = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)'])\n"
        f"open({str(marker)!r}, 'w').write(str(p.pid))\n"
        "time.sleep(120)\n"
    )
    with ToolLayer(root, ["python"], backend=local(root), limits=ToolLimits(command_timeout_s=1.5)) as tl:
        t0 = time.monotonic()
        r = tl.dispatch("run_python", {"code": code})
        elapsed = time.monotonic() - t0
    assert "TIMED_OUT" in r.content and elapsed < 15, r.content
    child_pid = int(marker.read_text())
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and _pid_alive(child_pid):
        time.sleep(0.2)
    assert not _pid_alive(child_pid), "grandchild survived the tree kill"


def test_deadline_and_cancel_bound_commands(root: Path):
    # deadline clamps the timeout: a 60 s command dies when the attempt's runtime ends
    with ToolLayer(root, ["python"], backend=local(root), deadline=time.monotonic() + 1.0) as tl:
        t0 = time.monotonic()
        r = tl.dispatch("run_python", {"code": "import time; time.sleep(30)", "timeout_s": 60})
        assert "TIMED_OUT" in r.content and time.monotonic() - t0 < 10
        r = tl.dispatch("run_python", {"code": "print(1)"})  # runtime exhausted → refused before it starts
        assert r.is_error and "runtime exhausted" in r.content
    # cancel event set mid-command → killed and reported CANCELLED; further commands refused
    cancel = threading.Event()
    with ToolLayer(root, ["python"], backend=local(root), cancel=cancel) as tl:
        threading.Timer(0.5, cancel.set).start()
        t0 = time.monotonic()
        r = tl.dispatch("run_python", {"code": "import time; time.sleep(30)"})
        assert "CANCELLED" in r.content and time.monotonic() - t0 < 10
        r = tl.dispatch("run_python", {"code": "print('no')"})
        assert r.is_error and "cancelled" in r.content


def test_run_bounded_direct(root: Path):
    res = run_bounded(
        [sys.executable, "-c", "print('ok')"], cwd=root, env=sanitized_env(root), timeout_s=10, max_output_bytes=100
    )
    assert res.exit_code == 0 and res.output.strip() == "ok" and not (res.timed_out or res.truncated or res.cancelled)
    assert res.render().startswith("exit_code=0")


def test_pytest_and_git_tools(root: Path):
    (root / "test_x.py").write_text("def test_ok():\n    assert 1 + 1 == 2\n", encoding="utf-8")
    with ToolLayer(root, ["python", "git"], backend=local(root)) as tl:
        r = tl.dispatch("run_pytest", {"args": ["test_x.py"]})
        assert "exit_code=0" in r.content and "1 passed" in r.content, r.content
        r = tl.dispatch("run_pytest", {"args": ["--rootdir=/", "test_x.py"]})
        assert r.is_error and "not allowed" in r.content
        subprocess.run(["git", "init", "-q"], cwd=root, check=True)
        subprocess.run(["git", "-c", "user.email=t@t", "-c", "user.name=t", "add", "-A"], cwd=root, check=True)
        subprocess.run(["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "base"], cwd=root, check=True)
        (root / "src" / "app.py").write_text("print('changed')\n", encoding="utf-8")
        st = tl.dispatch("git_status", {})
        assert "exit_code=0" in st.content and "src/app.py" in st.content
        d = tl.dispatch("git_diff", {"path": "src/app.py"})
        assert "+print('changed')" in d.content
        r = tl.dispatch("git_diff", {"path": "../x"})
        assert r.is_error
