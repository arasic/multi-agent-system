"""Step 7B: trusted acceptance adapters (ADR-007 §4a).

Host-side: the typed contract schema rejects everything unmappable; the verifier enforces the trusted runner
command and expected_checks == contract ids. Sandbox-side (Docker): the four adapters produce the right verdicts
against known-good / no-tests / broken-endpoint / volatile fixture apps.
"""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pytest

from mas.verifier.acceptance import AcceptanceVerifier, InvalidSuite, SandboxLimits
from mas.verifier.adapters import (
    TRUSTED_RUNNER_COMMAND,
    InvalidContract,
    parse_contract,
)
from mas.verifier.base import VerificationRequest, VerificationStatus
from tests.test_acceptance import ROOT, SPECIAL_SUITES, _fixture_commit, _request

CONTRACT_SUITE = ROOT / "acceptance" / "url_shortener_contract"


def _good() -> dict:
    return json.loads((CONTRACT_SUITE / "contract.json").read_text(encoding="utf-8"))


# ----------------------------------------------------------------------------- schema (no Docker)


def test_good_contract_parses_to_typed_checks():
    c = parse_contract(_good())
    assert [type(x).__name__ for x in c.checks] == [
        "BuildSucceeds",
        "TestsRequired",
        "HttpStatus",
        "HttpStatus",
        "HttpStatus",
        "RestartPersists",
    ]
    assert c.check_ids == ["compiles", "tests_pass", "health_ok", "shorten_created", "resolve_redirects", "survives_restart"]
    assert c.service is not None and c.service.port == 18080 and c.needs_service
    assert c.checks[5].expect.json_contains == {"urls": 1}


@pytest.mark.parametrize(
    "mutate, needle",
    [
        (lambda d: d["checks"].append({"id": "vibes", "type": "llm_judgement", "prompt": "ok?"}), "not a trusted adapter"),
        (lambda d: d["checks"][0].update({"shell": "rm -rf /"}), "unknown field"),
        (lambda d: d["checks"][0].update({"id": "compiles"}) or d["checks"].append(dict(d["checks"][0])), "unique"),
        (lambda d: d["checks"][0].update({"id": "../evil"}), "id must match"),
        (lambda d: d.__setitem__("service", None), "require a `service`"),
        (lambda d: d["checks"][2]["request"].update({"method": "TRACE"}), "method must be one of"),
        (lambda d: d["checks"][2]["expect"].update({"status": 999}), "HTTP status code"),
        (lambda d: d["service"].__setitem__("start", ["python", "app.py", "{secret}"]), "unsupported placeholder"),
        (lambda d: d["checks"][0].update({"timeout_s": 0}), "timeout_s must be"),
        (lambda d: d["checks"][1].update({"runner": "make test"}), "unmappable test runner"),
        (lambda d: d["checks"][5].update({"setup": []}), "non-empty array"),
        (lambda d: d.__setitem__("protocol_version", 2), "protocol_version"),
        (lambda d: d.__setitem__("checks", []), "non-empty array"),
        (lambda d: d["service"].__setitem__("port", 80), "port must be"),
    ],
)
def test_unmappable_or_malformed_contracts_are_rejected(mutate, needle):
    d = _good()
    mutate(d)
    with pytest.raises(InvalidContract, match=needle):
        parse_contract(d)


def test_service_without_service_checks_is_rejected():
    d = _good()
    d["checks"] = [d["checks"][0]]  # only build_succeeds, but service still declared
    with pytest.raises(InvalidContract, match="no check uses it"):
        parse_contract(d)


# ----------------------------------------------------------------------------- suite-level enforcement (no Docker)


def test_contract_suite_must_use_trusted_runner_and_matching_ids(tmp_path):
    root = tmp_path / "acc"
    suite = root / "bench"
    suite.mkdir(parents=True)
    (suite / "contract.json").write_text(json.dumps(_good()), encoding="utf-8")
    ids = parse_contract(_good()).check_ids
    good_manifest = {"protocol_version": 1, "command": TRUSTED_RUNNER_COMMAND, "expected_checks": ids, "timeout_s": 200}
    v = AcceptanceVerifier(root, image="irrelevant")

    (suite / "suite.json").write_text(json.dumps(good_manifest), encoding="utf-8")
    assert len(v.suite_digest("bench")) == 64  # valid

    bad = dict(good_manifest, command=["python", "/acceptance/my_own_runner.py"])
    (suite / "suite.json").write_text(json.dumps(bad), encoding="utf-8")
    with pytest.raises(InvalidSuite, match="trusted adapter runner"):
        v.suite_digest("bench")

    bad = dict(good_manifest, expected_checks=ids[:-1])
    (suite / "suite.json").write_text(json.dumps(bad), encoding="utf-8")
    with pytest.raises(InvalidSuite, match="expected_checks must equal"):
        v.suite_digest("bench")

    bad = dict(good_manifest, timeout_s=10)  # per-check timeouts (155 s) exceed the suite timeout
    (suite / "suite.json").write_text(json.dumps(bad), encoding="utf-8")
    with pytest.raises(InvalidSuite, match="exceeds the suite timeout"):
        v.suite_digest("bench")


def test_unknown_criterion_fails_closed_before_any_sandbox_work(tmp_path):
    repo, sha = _fixture_commit(tmp_path, "known_good")
    result = AcceptanceVerifier(SPECIAL_SUITES, image="image-does-not-matter").verify(
        _request(repo, sha, "contract_unknown_type")
    )
    assert result.status is VerificationStatus.INVALID
    assert "unmappable" in (result.reason or "") and "llm_judgement" in (result.reason or "")


# ----------------------------------------------------------------------------- adapters in the sandbox (Docker)


def _verify(tmp_path: Path, image: str, fixture: str):
    repo, sha = _fixture_commit(tmp_path, fixture)
    v = AcceptanceVerifier(ROOT / "acceptance", image=image, limits=SandboxLimits(timeout_s=300))
    return v.verify(VerificationRequest(uuid4(), "url_shortener_contract", repo, sha))


@pytest.mark.docker
def test_all_four_adapters_pass_on_a_good_app_with_tests(tmp_path, verifier_image):
    r = _verify(tmp_path, verifier_image, "known_good_with_tests")
    assert r.status is VerificationStatus.PASS, r.report
    by = {c.id: c for c in r.checks}
    assert by["compiles"].status.value == "PASS"
    assert by["tests_pass"].status.value == "PASS" and "2 test(s) passed via pytest" in by["tests_pass"].detail
    assert by["health_ok"].status.value == "PASS"
    assert by["shorten_created"].status.value == "PASS"
    assert by["resolve_redirects"].status.value == "PASS"
    assert by["survives_restart"].status.value == "PASS" and "restart" in by["survives_restart"].detail
    assert r.evidence["suite_sha256"] == AcceptanceVerifier(ROOT / "acceptance").suite_digest("url_shortener_contract")


@pytest.mark.docker
def test_tests_required_fails_when_the_app_has_no_tests(tmp_path, verifier_image):
    r = _verify(tmp_path, verifier_image, "known_good")  # same app, no test file
    assert r.status is VerificationStatus.FAIL
    by = {c.id: c for c in r.checks}
    assert by["tests_pass"].status.value == "FAIL" and "no tests were collected" in by["tests_pass"].detail
    # everything else about the app is fine — the verdict is precisely about the missing tests
    assert all(
        by[k].status.value == "PASS"
        for k in ("compiles", "health_ok", "shorten_created", "resolve_redirects", "survives_restart")
    )


@pytest.mark.docker
def test_http_status_fails_on_broken_endpoint(tmp_path, verifier_image):
    r = _verify(tmp_path, verifier_image, "failing_endpoint")
    assert r.status is VerificationStatus.FAIL
    by = {c.id: c for c in r.checks}
    assert by["shorten_created"].status.value == "FAIL" and "expected status 201" in by["shorten_created"].detail


@pytest.mark.docker
def test_restart_persists_fails_on_a_volatile_app(tmp_path, verifier_image):
    r = _verify(tmp_path, verifier_image, "forgets_on_restart")
    assert r.status is VerificationStatus.FAIL
    by = {c.id: c for c in r.checks}
    assert by["health_ok"].status.value == "PASS" and by["shorten_created"].status.value == "PASS"
    assert by["survives_restart"].status.value == "FAIL"
    assert "json['urls'] == 1" in by["survives_restart"].detail  # state was lost across the restart
