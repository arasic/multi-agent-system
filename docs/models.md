# Models

**Dated: 2026-08-16. Non-load-bearing.** Nothing in `mas/` outside `providers/` and config may depend on anything on this page. The architecture sees only `ModelProvider` (step 9 implemented: `mas/providers/` — `anthropic`, `openai`-compatible, `fake`; metered, priced from config, per-call telemetry in `model_calls`).

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

## Configuration (step 9 — the only place model names and prices are allowed)

```
MAS_MODEL_PLANNER=anthropic:claude-opus-5        # "<provider>:<model>"; providers: anthropic | openai | fake; empty = no model
MAS_MODEL_WORKER=openai:<model>                  # e.g. a fast/cheap model behind an OpenAI-compatible endpoint or gateway
MAS_MODEL_REVIEWER=                              # optional third role
MAS_MODEL_PRICES='{"claude-opus-5": {"input": 5, "output": 25, "cache_read": 0.5, "cache_write": 6.25},
                   "<model>": [in_per_Mtok, out_per_Mtok]}'   # USD per 1M tokens; keys may be id prefixes
MAS_ANTHROPIC_EFFORT=high                        # optional: low|medium|high|xhigh|max (API default when unset)
MAS_ANTHROPIC_THINKING=1                         # adaptive thinking on (default); 0 only for models that reject it
MAS_ANTHROPIC_FALLBACKS=                         # optional beta opt-in ("default"): server-side refusal fallbacks
MAS_OPENAI_BASE_URL=https://api.openai.com/v1    # or an in-cluster gateway; OPENAI_API_KEY / MAS_OPENAI_API_KEY
MAS_OPENAI_MAX_TOKENS_FIELD=max_completion_tokens  # older compatible servers want max_tokens
MAS_ATTEMPT_MAX_CALLS=40                         # per-attempt call budget (metered provider)
MAS_ATTEMPT_MAX_TOKENS=                          # optional worker-side ceiling; tokens per attempt are a RUN budget:
                                                 # --max-attempt-tokens (default 200000; validator rule 8 allocation unit)
```

Anthropic credentials resolve the SDK way (`ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`, or an `ant auth login` profile); in Compose the gateway is the only service that receives them, forwarded from your shell or `.env` **only when set** (an `ant auth login` profile lives on the host and does not reach the container — export a key or token for `mas up`). `mas models` prints the roles and which are unpriced; `mas models --ping --spec <provider>:<model>` makes one metered test call. `pip install -e ".[llm]"` installs the Anthropic SDK (the Compose image installs it too, so the `gateway` service can build a real upstream); the `openai` provider needs no SDK; `fake` needs nothing. The Compose `gateway` service receives `MAS_ANTHROPIC_THINKING/EFFORT/FALLBACKS` and `MAS_PROVIDER_TIMEOUT_S/MAX_RETRIES` from your shell or `.env`, so the distributed path sends the same request shape as a direct `mas run` — with adaptive thinking on, the signed thinking blocks of every tool round are replayed unchanged, directly and across the gateway wire.

### Prices on record

| Model id | Input $/M | Output $/M | Cache read $/M | Cache write $/M | Source / date | Status |
|---|---|---|---|---|---|---|
| `claude-opus-5` | 5.00 | 25.00 | 0.50 (est. 0.1×) | 6.25 (est. 1.25×) | Anthropic model table as cached in the Claude API skill, dated 2026-06-24 | ⚠ re-check against platform.claude.com/docs/en/pricing before any benchmark |
| `claude-sonnet-5` | 3.00 (intro 2.00 to 2026-08-31) | 15.00 (intro 10.00) | 0.1× | 1.25× | same | ⚠ same |
| `claude-haiku-4-5` | 1.00 | 5.00 | 0.1× | 1.25× | same | ⚠ same |
| GPT-5.6 Sol / Gemini 3.7 Flash (discussion) | — | — | — | — | discuss.md claims only | ✗ unverified; **leave unpriced** rather than guess |

Cache multipliers are the vendor's standard ratios, not per-model confirmations. Whatever ends up in `MAS_MODEL_PRICES` must be copied from the vendor's current page on the day, with the date recorded here.

## What we measure regardless of model

Per call (`model_calls`): provider, model, role, tokens (incl. cache), cost, `priced`, latency, stop reason / error. Per attempt: `model`, `input_tokens`, `output_tokens`, `cost_usd` (settled from the meter). Per run: totals; `mas status` shows both and flags unpriced usage. This is what makes configs A–D comparable when models change.
