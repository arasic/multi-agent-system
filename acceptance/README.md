# acceptance/

Fixed external acceptance suites, one directory per benchmark (`acceptance/<benchmark>/`).

Rules (ADR-003):
- Written by humans **before** a run. Never generated, moved, or modified by agents.
- Mounted **read-only** into every worker. Only `mas/verifier/` executes them.
- The verdict comes from here and nowhere else.

Populated at roadmap step 7 (`url_shortener/`, later `adapters/`).
