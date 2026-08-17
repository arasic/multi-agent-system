# fake_assets — canned outputs for the offline demo provider `fake:builder`

`fake:builder` is a **scripted double**, not a model: it reads the task brief and "builds" deterministically —
documents for `document:<name>` contracts, and, when the run goal names the URL shortener, this canned known-good
app + tests for implementation/testing tasks. It exists so the whole intelligence path (LLM worker loop → tool layer →
execution runner → sandbox → runtime commit → integration → external verifier) can be exercised end to end with **no
API key**, in compose. It proves the plumbing, never the intelligence. Everything under here is copied from
`tests/fixtures/apps/known_good_with_tests`.
