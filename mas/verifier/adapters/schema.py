"""Typed acceptance-contract schema (ADR-007 §4a) — the ONLY criterion types the trusted adapters can execute.

Stdlib-only on purpose: this file is copied into the verifier image and imported by the in-sandbox runner, so the
host (freeze time, verification time) and the sandbox validate contracts with the same code. Anything not
representable here is *unmappable* and must fail closed at approval time — never "skipped".

Contract shape (JSON):

    {
      "protocol_version": 1,
      "service": {                                    # required iff any http_status / restart_persists check
        "start": ["python", "app.py", "--port", "{port}", "--db", "{state_dir}/app.sqlite3"],
        "health": "/health",                          # GET must return 200 before checks run
        "port": 18080,
        "startup_timeout_s": 5
      },
      "checks": [
        {"id": "compiles",  "type": "build_succeeds",  "command": ["python", "-m", "py_compile", "app.py"], "timeout_s": 30},
        {"id": "tests",     "type": "tests_required",  "runner": "pytest", "args": ["-q"], "min_tests": 1, "timeout_s": 60},
        {"id": "shorten",   "type": "http_status",     "request": {"method": "POST", "path": "/shorten", "json": {"url": "https://e.com"}},
                                                        "expect": {"status": 201, "json_contains": {"code": "abc123"}}},
        {"id": "persists",  "type": "restart_persists","setup": [{"method": "POST", "path": "/shorten", "json": {"url": "https://e.com"}}],
                                                        "verify": {"method": "GET", "path": "/stats"},
                                                        "expect": {"status": 200, "json_contains": {"urls": 1}}}
      ]
    }

Placeholders allowed in `service.start`: {port}, {state_dir} (persists across restarts within one verification), {repo}.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

PROTOCOL_VERSION = 1
CHECK_ID = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"}
_RUNNERS = {"pytest", "unittest"}
_PLACEHOLDERS = {"{port}", "{state_dir}", "{repo}"}
MAX_CHECKS = 64
MAX_TIMEOUT_S = 600  # absolute ceiling; the suite's own timeout is the real bound at verification time


class InvalidContract(ValueError):
    """The contract is malformed or asks for something no trusted adapter can execute (unmappable)."""


# ----------------------------------------------------------------------------- typed criteria


@dataclass(frozen=True)
class HttpRequest:
    method: str
    path: str
    json: Any = None
    headers: dict[str, str] = field(default_factory=dict)
    timeout_s: float = 5.0

    def as_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"method": self.method, "path": self.path, "timeout_s": self.timeout_s}
        if self.json is not None:
            d["json"] = self.json
        if self.headers:
            d["headers"] = dict(self.headers)
        return d


@dataclass(frozen=True)
class HttpExpect:
    status: int
    json_contains: dict[str, Any] = field(default_factory=dict)  # top-level keys that must equal these values
    header_equals: dict[str, str] = field(default_factory=dict)  # header name -> exact value (case-insensitive name)

    def as_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"status": self.status}
        if self.json_contains:
            d["json_contains"] = dict(self.json_contains)
        if self.header_equals:
            d["header_equals"] = dict(self.header_equals)
        return d


@dataclass(frozen=True)
class Service:
    start: tuple[str, ...]
    health: str
    port: int
    startup_timeout_s: float = 5.0

    def as_dict(self) -> dict[str, Any]:
        return {"start": list(self.start), "health": self.health, "port": self.port, "startup_timeout_s": self.startup_timeout_s}


@dataclass(frozen=True)
class BuildSucceeds:
    id: str
    command: tuple[str, ...]
    timeout_s: int = 60
    type: str = "build_succeeds"


@dataclass(frozen=True)
class TestsRequired:
    id: str
    runner: str  # pytest | unittest
    args: tuple[str, ...] = ()
    min_tests: int = 1
    timeout_s: int = 120
    type: str = "tests_required"


@dataclass(frozen=True)
class HttpStatus:
    id: str
    request: HttpRequest
    expect: HttpExpect
    timeout_s: int = 30
    type: str = "http_status"


@dataclass(frozen=True)
class RestartPersists:
    id: str
    setup: tuple[HttpRequest, ...]
    verify: HttpRequest
    expect: HttpExpect
    timeout_s: int = 60
    type: str = "restart_persists"


Check = BuildSucceeds | TestsRequired | HttpStatus | RestartPersists
CHECK_TYPES = ("build_succeeds", "tests_required", "http_status", "restart_persists")
SERVICE_CHECK_TYPES = {"http_status", "restart_persists"}


@dataclass(frozen=True)
class Contract:
    checks: tuple[Check, ...]
    service: Service | None = None

    @property
    def check_ids(self) -> list[str]:
        return [c.id for c in self.checks]

    @property
    def needs_service(self) -> bool:
        return any(c.type in SERVICE_CHECK_TYPES for c in self.checks)


# ----------------------------------------------------------------------------- parsing / validation


def _keys(d: dict[str, Any], allowed: set[str], required: set[str], where: str) -> None:
    if not isinstance(d, dict):
        raise InvalidContract(f"{where} must be an object")
    extra = set(d) - allowed
    missing = required - set(d)
    if extra:
        raise InvalidContract(f"{where} has unknown field(s): {sorted(extra)}")
    if missing:
        raise InvalidContract(f"{where} is missing required field(s): {sorted(missing)}")


def _str_list(v: Any, where: str, *, allow_placeholders: bool = False) -> tuple[str, ...]:
    if not isinstance(v, list) or not v or not all(isinstance(x, str) and x for x in v):
        raise InvalidContract(f"{where} must be a non-empty array of non-empty strings")
    for x in v:
        for ph in re.findall(r"\{[^}]*\}", x):
            if not allow_placeholders or ph not in _PLACEHOLDERS:
                raise InvalidContract(f"{where} contains an unsupported placeholder {ph!r}")
        if "\x00" in x or "\n" in x:
            raise InvalidContract(f"{where} contains a control character")
    return tuple(v)


def _timeout(v: Any, default: int, where: str) -> int:
    if v is None:
        return default
    if not isinstance(v, int) or isinstance(v, bool) or not 1 <= v <= MAX_TIMEOUT_S:
        raise InvalidContract(f"{where}.timeout_s must be an integer in [1, {MAX_TIMEOUT_S}]")
    return v


def _path(v: Any, where: str) -> str:
    if not isinstance(v, str) or not v.startswith("/") or any(c.isspace() for c in v) or "\x00" in v:
        raise InvalidContract(f"{where} must be an absolute URL path without whitespace")
    return v


def _request(d: Any, where: str) -> HttpRequest:
    _keys(d, {"method", "path", "json", "headers", "timeout_s"}, {"method", "path"}, where)
    method = d["method"]
    if method not in _METHODS:
        raise InvalidContract(f"{where}.method must be one of {sorted(_METHODS)}")
    headers = d.get("headers", {})
    if not isinstance(headers, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in headers.items()):
        raise InvalidContract(f"{where}.headers must be an object of strings")
    t = d.get("timeout_s", 5.0)
    if not isinstance(t, int | float) or isinstance(t, bool) or not 0.1 <= float(t) <= 60:
        raise InvalidContract(f"{where}.timeout_s must be a number in [0.1, 60]")
    body = d.get("json")
    if body is not None and not isinstance(body, dict | list | str | int | float | bool):
        raise InvalidContract(f"{where}.json must be JSON-serialisable")
    return HttpRequest(
        method=method, path=_path(d["path"], f"{where}.path"), json=body, headers=dict(headers), timeout_s=float(t)
    )


def _expect(d: Any, where: str) -> HttpExpect:
    _keys(d, {"status", "json_contains", "header_equals"}, {"status"}, where)
    st = d["status"]
    if not isinstance(st, int) or isinstance(st, bool) or not 100 <= st <= 599:
        raise InvalidContract(f"{where}.status must be an HTTP status code")
    jc = d.get("json_contains", {})
    if not isinstance(jc, dict) or not all(isinstance(k, str) for k in jc):
        raise InvalidContract(f"{where}.json_contains must be an object")
    he = d.get("header_equals", {})
    if not isinstance(he, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in he.items()):
        raise InvalidContract(f"{where}.header_equals must be an object of strings")
    return HttpExpect(status=st, json_contains=dict(jc), header_equals=dict(he))


def _service(d: Any) -> Service:
    _keys(d, {"start", "health", "port", "startup_timeout_s"}, {"start", "health", "port"}, "service")
    port = d["port"]
    if not isinstance(port, int) or isinstance(port, bool) or not 1024 <= port <= 65535:
        raise InvalidContract("service.port must be an integer in [1024, 65535]")
    st = d.get("startup_timeout_s", 5.0)
    if not isinstance(st, int | float) or isinstance(st, bool) or not 0.5 <= float(st) <= 60:
        raise InvalidContract("service.startup_timeout_s must be a number in [0.5, 60]")
    return Service(
        start=_str_list(d["start"], "service.start", allow_placeholders=True),
        health=_path(d["health"], "service.health"),
        port=port,
        startup_timeout_s=float(st),
    )


def _check(d: Any, i: int) -> Check:
    where = f"checks[{i}]"
    if not isinstance(d, dict):
        raise InvalidContract(f"{where} must be an object")
    typ = d.get("type")
    cid = d.get("id")
    if not isinstance(cid, str) or not CHECK_ID.match(cid):
        raise InvalidContract(f"{where}.id must match {CHECK_ID.pattern}")
    where = f"checks[{i}]({cid})"
    if typ == "build_succeeds":
        _keys(d, {"id", "type", "command", "timeout_s"}, {"id", "type", "command"}, where)
        return BuildSucceeds(
            id=cid, command=_str_list(d["command"], f"{where}.command"), timeout_s=_timeout(d.get("timeout_s"), 60, where)
        )
    if typ == "tests_required":
        _keys(d, {"id", "type", "runner", "args", "min_tests", "timeout_s"}, {"id", "type", "runner"}, where)
        runner = d["runner"]
        if runner not in _RUNNERS:
            raise InvalidContract(f"{where}.runner must be one of {sorted(_RUNNERS)} (unmappable test runner)")
        args = tuple(_str_list(d["args"], f"{where}.args")) if d.get("args") else ()
        mt = d.get("min_tests", 1)
        if not isinstance(mt, int) or isinstance(mt, bool) or not 1 <= mt <= 100_000:
            raise InvalidContract(f"{where}.min_tests must be a positive integer")
        return TestsRequired(id=cid, runner=runner, args=args, min_tests=mt, timeout_s=_timeout(d.get("timeout_s"), 120, where))
    if typ == "http_status":
        _keys(d, {"id", "type", "request", "expect", "timeout_s"}, {"id", "type", "request", "expect"}, where)
        return HttpStatus(
            id=cid,
            request=_request(d["request"], f"{where}.request"),
            expect=_expect(d["expect"], f"{where}.expect"),
            timeout_s=_timeout(d.get("timeout_s"), 30, where),
        )
    if typ == "restart_persists":
        _keys(d, {"id", "type", "setup", "verify", "expect", "timeout_s"}, {"id", "type", "setup", "verify", "expect"}, where)
        setup = d["setup"]
        if not isinstance(setup, list) or not setup or len(setup) > 32:
            raise InvalidContract(f"{where}.setup must be a non-empty array (max 32 requests)")
        return RestartPersists(
            id=cid,
            setup=tuple(_request(r, f"{where}.setup[{j}]") for j, r in enumerate(setup)),
            verify=_request(d["verify"], f"{where}.verify"),
            expect=_expect(d["expect"], f"{where}.expect"),
            timeout_s=_timeout(d.get("timeout_s"), 60, where),
        )
    raise InvalidContract(f"{where}.type {typ!r} is not a trusted adapter (allowed: {list(CHECK_TYPES)}) — unmappable")


def parse_contract(doc: Any) -> Contract:
    """Parse + validate an ADR-007 acceptance contract. Raises InvalidContract for anything unmappable."""
    _keys(doc, {"protocol_version", "service", "checks"}, {"protocol_version", "checks"}, "contract")
    if doc["protocol_version"] != PROTOCOL_VERSION:
        raise InvalidContract("unknown contract protocol_version")
    raw_checks = doc["checks"]
    if not isinstance(raw_checks, list) or not raw_checks or len(raw_checks) > MAX_CHECKS:
        raise InvalidContract(f"checks must be a non-empty array (max {MAX_CHECKS})")
    checks = tuple(_check(c, i) for i, c in enumerate(raw_checks))
    ids = [c.id for c in checks]
    if len(set(ids)) != len(ids):
        raise InvalidContract("check ids must be unique")
    service = _service(doc["service"]) if doc.get("service") is not None else None
    contract = Contract(checks=checks, service=service)
    if contract.needs_service and service is None:
        raise InvalidContract("http_status / restart_persists checks require a `service` definition")
    if service is not None and not contract.needs_service:
        raise InvalidContract("`service` is defined but no check uses it")
    return contract
