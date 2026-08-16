# Models

**Dated: 2026-08-16. Non-load-bearing.** Nothing in `mas/` outside `providers/` and config may depend on anything on this page. The architecture sees only `ModelProvider`.

## Roles in the MVP (three, no dynamic routing)

| Role | Requirement | Where used |
|---|---|---|
| **planner / re-planner** | strongest reasoning available; typed JSON output | `mas/planner/planner.py` |
| **worker** | fast, cheap, good at coding and multi-step tool use; high volume | worker agents |
| **reviewer** (later) | strong, preferably a *different family* from the planner for diverse failure modes | conflict resolution, post-MVP |

## Candidate assignments (as proposed in design discussion — **unverified**)

| Role | Proposed model | Claimed properties | Verification status |
|---|---|---|---|
| planner | GPT-5.6 Sol (Ultrafast tier) | up to ~750 output tok/s | ⚠ not verified against provider docs |
| worker | Gemini 3.7 Flash | positioned for coding/agents; introductory API pricing $0.75/M in, $3.75/M out through 2026-12-31 | ⚠ not verified against provider docs |
| reviewer | Claude (Opus/Sonnet-class) | independent family | ⚠ not verified |
| tiny/routine (later) | Flash-Lite / Luna-class | high throughput | ⚠ not verified |

**Before wiring any provider:** confirm the model id, pricing, rate limits, and context window against the provider's current documentation, and record the confirmed values here with the date. Model names and prices go in config, not code.

## What we measure regardless of model

Per attempt: `model`, `input_tokens`, `output_tokens`, `cost_usd`. Per run: totals. This is what makes configs A–D comparable when models change.
