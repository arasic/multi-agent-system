# Width benchmark

The frozen M3 task family implements `N = 1/2/4/8/16` independent Python adapters against immutable suites
`acceptance/adapters_<N>`. Each adapter has one disjoint module and one affine mapping, so the benchmark exposes real
parallel width without hiding integration behind an underspecified goal.

The hand-written DAG fixtures are generated deterministically by `mas.evaluation.width_dag(N)` and contain scripted
stub outputs for the key-less substrate rehearsal. Real C/D runs receive the same textual goal through the LLM planner;
A/B are transformed by the runtime into one `solve` task plus system integration.

Run the whole matrix with `python scripts/benchmark.py`; use `--offline --repeats 1` to rehearse the pipeline and the
real model arguments with at least five repetitions for evidence. Results are append-only JSONL plus CSV, JSON and SVG.
