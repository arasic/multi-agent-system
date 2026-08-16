# Fixed acceptance suites

This directory is trusted verifier input, never agent output. Each benchmark has a
`suite.json` manifest and a deterministic runner. The directory is hashed before every
verification and mounted read-only in the sandbox. Workers must never receive write
access to it.

Build the small, dependency-free sandbox image once:

```sh
docker build -f acceptance/Dockerfile.verifier -t mas-verifier:latest .
```

The URL-shortener contract is intentionally narrow for the first PoC: the integration
commit must contain a root `app.py` accepting `--port` and `--db`. It must implement the
health, shorten, resolve and stats behaviours exercised by the fixed suite.

## Contract-based suites (ADR-007 §4a, trusted adapters)

A suite may be an approved **acceptance contract** instead of a hand-written runner:

```
acceptance/<benchmark>/
  suite.json      command MUST be the trusted runner: ["python", "/opt/mas/adapters/runner.py", "/acceptance/contract.json"]
                  expected_checks MUST equal the contract's check ids, in order
  contract.json   protocol_version, optional service {start, health, port, startup_timeout_s}, checks [...]
```

Only four typed criterion types exist — `build_succeeds`, `tests_required` (runner ∈ pytest|unittest, min_tests),
`http_status` (request → expect status / json_contains / header_equals), `restart_persists` (setup requests →
service restart → verify request). Anything else is **unmappable** and rejected at approval time
(`mas contract <file>`) and again at verification time. The adapters live in the verifier image
(`/opt/mas/adapters/`, from `mas/verifier/adapters/`), so the image digest recorded in every verification artifact
pins the adapter code; the suite digest pins the contract. Example: `url_shortener_contract/`.

