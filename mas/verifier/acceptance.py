"""Fail-closed Docker acceptance runner (ADR-003, ADR-007).

Agent-authored code is archived from the exact integration commit and mounted read-only.
The fixed suite is independently hashed and mounted read-only.  Execution happens with
no network, a read-only root filesystem, no capabilities, no-new-privileges, bounded
CPU/memory/PIDs/output, disposable tmpfs storage, and a host-enforced wall-clock limit.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
import tarfile
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mas.verifier.base import CheckResult, CheckStatus, VerificationRequest, VerificationResult, VerificationStatus

_SAFE_BENCHMARK = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
_FULL_SHA = re.compile(r"^[0-9a-f]{40,64}$")
_MANIFEST_KEYS = {"protocol_version", "command", "expected_checks", "timeout_s"}
_MAX_REPORT_BYTES = 256 * 1024


@dataclass(frozen=True)
class SandboxLimits:
    timeout_s: int = 30
    cpus: float = 1.0
    memory_mb: int = 256
    pids: int = 128
    tmpfs_mb: int = 192
    max_source_bytes: int = 100 * 1024 * 1024
    max_source_files: int = 10_000
    max_output_bytes: int = _MAX_REPORT_BYTES

    def as_dict(self) -> dict[str, Any]:
        return {
            "timeout_s": self.timeout_s,
            "cpus": self.cpus,
            "memory_mb": self.memory_mb,
            "pids": self.pids,
            "tmpfs_mb": self.tmpfs_mb,
            "max_source_bytes": self.max_source_bytes,
            "max_source_files": self.max_source_files,
            "max_output_bytes": self.max_output_bytes,
        }


class AcceptanceVerifier:
    name = "acceptance-docker-v1"

    def __init__(
        self,
        acceptance_root: str | Path = "acceptance",
        *,
        image: str = "mas-verifier:latest",
        limits: SandboxLimits | None = None,
        docker: str = "docker",
    ):
        self.acceptance_root = Path(acceptance_root).resolve()
        self.image = image
        self.limits = limits or SandboxLimits()
        self.docker = docker

    def verify(self, request: VerificationRequest) -> VerificationResult:
        started = time.monotonic()
        try:
            return self._verify(request, started)
        except Exception as exc:  # every runner defect is evidence, never a stranded VERIFYING run
            return self._failed(
                request,
                VerificationStatus.ERROR,
                f"unexpected verifier error: {type(exc).__name__}: {exc}",
                started,
            )

    def _verify(self, request: VerificationRequest, started: float) -> VerificationResult:
        invalid = self._validate_request(request)
        if invalid:
            return invalid
        assert request.benchmark and request.repository and request.commit_sha

        try:
            suite, manifest, suite_hash = self._load_suite(request.benchmark)
            image_ref = self._image_ref()
        except InvalidSuite as exc:
            return self._failed(request, VerificationStatus.INVALID, str(exc), started)
        except RunnerUnavailable as exc:
            return self._failed(request, VerificationStatus.ERROR, str(exc), started)

        with tempfile.TemporaryDirectory(prefix=f"mas-verify-{str(request.run_id)[:8]}-") as tmp:
            root = Path(tmp)
            checkout = root / "input"
            checkout.mkdir()
            try:
                self._checkout_exact(request.repository, request.commit_sha, checkout)
            except (InvalidCommit, RunnerUnavailable) as exc:
                status = VerificationStatus.INVALID if isinstance(exc, InvalidCommit) else VerificationStatus.ERROR
                return self._failed(request, status, str(exc), started, suite_hash=suite_hash, image_ref=image_ref)

            outcome = self._run_container(checkout, suite, manifest, request, suite_hash, image_ref, root)

        # A writable or externally replaced suite is never silently accepted.
        if _hash_tree(suite) != suite_hash:
            return self._failed(
                request,
                VerificationStatus.INVALID,
                "acceptance suite changed during verification",
                started,
                suite_hash=suite_hash,
                image_ref=image_ref,
            )
        return outcome

    def _validate_request(self, request: VerificationRequest) -> VerificationResult | None:
        if not request.benchmark or not _SAFE_BENCHMARK.fullmatch(request.benchmark):
            return VerificationResult.fail(
                "missing or unsafe benchmark id", status=VerificationStatus.INVALID, evidence={"run_id": str(request.run_id)}
            )
        if request.repository is None or not request.repository.is_dir():
            return VerificationResult.fail(
                "integration repository is missing", status=VerificationStatus.INVALID, evidence={"run_id": str(request.run_id)}
            )
        if not request.commit_sha or not _FULL_SHA.fullmatch(request.commit_sha):
            return VerificationResult.fail(
                "integration commit is missing or is not a full object id",
                status=VerificationStatus.INVALID,
                evidence={"run_id": str(request.run_id)},
            )
        return None

    def _load_suite(self, benchmark: str) -> tuple[Path, dict[str, Any], str]:
        suite = (self.acceptance_root / benchmark).resolve()
        if suite.parent != self.acceptance_root or not suite.is_dir():
            raise InvalidSuite(f"acceptance suite not found: {benchmark}")
        manifest_path = suite / "suite.json"
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise InvalidSuite(f"invalid suite manifest: {exc}") from exc
        if not isinstance(manifest, dict) or set(manifest) != _MANIFEST_KEYS:
            raise InvalidSuite(f"suite manifest keys must be exactly {sorted(_MANIFEST_KEYS)}")
        if manifest["protocol_version"] != 1:
            raise InvalidSuite("unknown suite protocol_version")
        command = manifest["command"]
        if not isinstance(command, list) or not command or not all(isinstance(x, str) and x for x in command):
            raise InvalidSuite("suite command must be a non-empty string array")
        checks = manifest["expected_checks"]
        if (
            not isinstance(checks, list)
            or not checks
            or not all(isinstance(x, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,63}", x) for x in checks)
            or len(set(checks)) != len(checks)
        ):
            raise InvalidSuite("expected_checks must be a non-empty unique check-id array")
        timeout_s = manifest["timeout_s"]
        if not isinstance(timeout_s, int) or not 1 <= timeout_s <= self.limits.timeout_s:
            raise InvalidSuite(f"suite timeout_s must be between 1 and {self.limits.timeout_s}")
        return suite, manifest, _hash_tree(suite)

    def _image_ref(self) -> str:
        try:
            p = subprocess.run(
                [self.docker, "image", "inspect", self.image], capture_output=True, text=True, timeout=15, check=False
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RunnerUnavailable(f"Docker runner unavailable: {exc}") from exc
        if p.returncode != 0:
            raise RunnerUnavailable(f"sandbox image {self.image!r} is not present (build acceptance/Dockerfile.verifier)")
        try:
            info = json.loads(p.stdout)[0]
            digests = info.get("RepoDigests") or []
            return str(digests[0] if digests else info["Id"])
        except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
            raise RunnerUnavailable("Docker returned an invalid image inspection result") from exc

    def _checkout_exact(self, repository: Path, sha: str, checkout: Path) -> None:
        git = shutil.which("git")
        if git is None:
            raise RunnerUnavailable("git is not installed")
        try:
            resolved = subprocess.run(
                [git, "--git-dir", str(repository), "rev-parse", "--verify", f"{sha}^{{commit}}"],
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RunnerUnavailable(f"git could not inspect integration commit: {exc}") from exc
        if resolved.returncode != 0 or resolved.stdout.strip() != sha:
            raise InvalidCommit("integration commit does not exist as the exact requested commit")

        listing = subprocess.run(
            [git, "--git-dir", str(repository), "ls-tree", "-rl", sha],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        if listing.returncode != 0:
            raise InvalidCommit("cannot enumerate integration commit")
        sizes = []
        for line in listing.stdout.splitlines():
            try:
                sizes.append(int(line.split(maxsplit=4)[3]))
            except (IndexError, ValueError):
                raise InvalidCommit("integration commit contains an invalid tree entry") from None
        if len(sizes) > self.limits.max_source_files or sum(sizes) > self.limits.max_source_bytes:
            raise InvalidCommit("integration commit exceeds verifier source limits")

        archive = checkout.parent / "source.tar"
        made = subprocess.run(
            [git, "--git-dir", str(repository), "archive", "--format=tar", "-o", str(archive), sha],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if made.returncode != 0:
            raise InvalidCommit(f"cannot archive integration commit: {made.stderr.strip()}")
        try:
            with tarfile.open(archive) as tf:
                tf.extractall(checkout, filter="data")
        except (OSError, tarfile.TarError) as exc:
            raise InvalidCommit(f"cannot extract integration commit: {exc}") from exc

    def _run_container(
        self,
        checkout: Path,
        suite: Path,
        manifest: dict[str, Any],
        request: VerificationRequest,
        suite_hash: str,
        image_ref: str,
        temp_root: Path,
    ) -> VerificationResult:
        name = f"mas-verify-{uuid.uuid4().hex[:16]}"
        timeout_s = int(manifest["timeout_s"])
        memory = f"{self.limits.memory_mb}m"
        tmpfs = f"rw,size={self.limits.tmpfs_mb}m,uid=10001,gid=10001,mode=0700"
        cmd = [
            self.docker,
            "run",
            "--name",
            name,
            "--pull",
            "never",
            "--network",
            "none",
            "--read-only",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--pids-limit",
            str(self.limits.pids),
            "--memory",
            memory,
            "--memory-swap",
            memory,
            "--cpus",
            str(self.limits.cpus),
            "--user",
            "10001:10001",
            "--tmpfs",
            f"/work:{tmpfs}",
            "--tmpfs",
            "/tmp:rw,size=16m,uid=10001,gid=10001,mode=0700",
            "--mount",
            f"type=bind,src={checkout.resolve()},dst=/input,readonly",
            "--mount",
            f"type=bind,src={suite.resolve()},dst=/acceptance,readonly",
            self.image,
            *manifest["command"],
        ]
        stdout_path, stderr_path = temp_root / "stdout", temp_root / "stderr"
        started = time.monotonic()
        try:
            with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
                proc = subprocess.Popen(cmd, stdout=stdout, stderr=stderr)
                try:
                    exit_code = proc.wait(timeout=timeout_s)
                except subprocess.TimeoutExpired:
                    subprocess.run([self.docker, "rm", "-f", name], capture_output=True, timeout=15, check=False)
                    proc.wait(timeout=15)
                    return self._failed(
                        request,
                        VerificationStatus.TIMEOUT,
                        f"acceptance suite exceeded hard timeout of {timeout_s}s",
                        started,
                        suite_hash=suite_hash,
                        image_ref=image_ref,
                    )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return self._failed(
                request,
                VerificationStatus.ERROR,
                f"sandbox runner crashed: {exc}",
                started,
                suite_hash=suite_hash,
                image_ref=image_ref,
            )
        finally:
            subprocess.run([self.docker, "rm", "-f", name], capture_output=True, timeout=15, check=False)

        stdout = _read_capped(stdout_path, self.limits.max_output_bytes)
        stderr = _read_capped(stderr_path, self.limits.max_output_bytes)
        evidence = self._evidence(request, started, suite_hash, image_ref)
        evidence.update({"exit_code": exit_code, "stderr": stderr})
        if stdout_path.stat().st_size > self.limits.max_output_bytes or stderr_path.stat().st_size > self.limits.max_output_bytes:
            return VerificationResult.fail(
                "acceptance output exceeded limit", status=VerificationStatus.INVALID, evidence=evidence
            )
        if exit_code != 0:
            evidence["stdout"] = stdout
            return VerificationResult.fail(
                f"acceptance runner exited with code {exit_code}", status=VerificationStatus.ERROR, evidence=evidence
            )
        return self._parse_report(stdout, manifest["expected_checks"], evidence)

    def _parse_report(self, raw: str, expected: list[str], evidence: dict[str, Any]) -> VerificationResult:
        try:
            report = json.loads(raw)
        except json.JSONDecodeError as exc:
            return VerificationResult.fail(
                f"acceptance runner emitted invalid JSON: {exc}", status=VerificationStatus.INVALID, evidence=evidence
            )
        if not isinstance(report, dict) or set(report) != {"protocol_version", "checks"} or report["protocol_version"] != 1:
            return VerificationResult.fail(
                "acceptance report has an invalid envelope", status=VerificationStatus.INVALID, evidence=evidence
            )
        rows = report["checks"]
        if not isinstance(rows, list):
            return VerificationResult.fail(
                "acceptance report checks must be an array", status=VerificationStatus.INVALID, evidence=evidence
            )
        checks: list[CheckResult] = []
        try:
            for row in rows:
                if not isinstance(row, dict) or not {"id", "status"} <= set(row) or not set(row) <= {
                    "id",
                    "status",
                    "detail",
                    "duration_ms",
                }:
                    raise ValueError("invalid check record")
                checks.append(
                    CheckResult(
                        id=row["id"],
                        status=CheckStatus(row["status"]),
                        detail=str(row.get("detail", ""))[:4000],
                        duration_ms=int(row["duration_ms"]) if "duration_ms" in row else None,
                    )
                )
        except (KeyError, TypeError, ValueError) as exc:
            return VerificationResult.fail(
                f"acceptance report contains an invalid check: {exc}",
                status=VerificationStatus.INVALID,
                evidence=evidence,
            )
        actual = [c.id for c in checks]
        if len(set(actual)) != len(actual) or set(actual) != set(expected):
            evidence["expected_checks"] = expected
            evidence["reported_checks"] = actual
            return VerificationResult.fail(
                "acceptance report has missing, duplicate, or unknown checks",
                status=VerificationStatus.INVALID,
                checks=tuple(checks),
                evidence=evidence,
            )
        ordered = tuple(next(c for c in checks if c.id == check_id) for check_id in expected)
        if all(c.status is CheckStatus.PASS for c in ordered):
            return VerificationResult.pass_(checks=ordered, evidence=evidence)
        return VerificationResult.fail("one or more acceptance checks failed", checks=ordered, evidence=evidence)

    def _failed(
        self,
        request: VerificationRequest,
        status: VerificationStatus,
        reason: str,
        started: float,
        *,
        suite_hash: str | None = None,
        image_ref: str | None = None,
    ) -> VerificationResult:
        return VerificationResult.fail(
            reason,
            status=status,
            evidence=self._evidence(request, started, suite_hash, image_ref),
        )

    def _evidence(
        self,
        request: VerificationRequest,
        started: float,
        suite_hash: str | None,
        image_ref: str | None,
    ) -> dict[str, Any]:
        return {
            "run_id": str(request.run_id),
            "benchmark": request.benchmark,
            "integration_sha": request.commit_sha,
            "suite_sha256": suite_hash,
            "sandbox_image": image_ref,
            "duration_ms": round((time.monotonic() - started) * 1000),
            "limits": self.limits.as_dict(),
        }


class InvalidSuite(Exception):
    pass


class InvalidCommit(Exception):
    pass


class RunnerUnavailable(Exception):
    pass


def _hash_tree(root: Path) -> str:
    h = hashlib.sha256()
    for path in sorted(p for p in root.rglob("*") if p.is_file() and "__pycache__" not in p.parts):
        rel = path.relative_to(root).as_posix().encode()
        h.update(len(rel).to_bytes(4, "big"))
        h.update(rel)
        data = path.read_bytes()
        h.update(len(data).to_bytes(8, "big"))
        h.update(data)
    return h.hexdigest()


def _read_capped(path: Path, limit: int) -> str:
    with path.open("rb") as f:
        return f.read(limit).decode("utf-8", errors="replace")
