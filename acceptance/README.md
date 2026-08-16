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
