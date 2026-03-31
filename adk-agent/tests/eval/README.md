# ADK Eval Harness

This directory contains the first minimal ADK eval harness for AI Co-Scientist.

## What is covered

- `evalsets/report_core.json`: report-mode basics for direct QA, clarification, and plan generation
- `evalsets/analysis_core.json`: analysis-mode basics for clarification and dataset-oriented planning
- `eval_config.json`: shared semantic-quality criteria for the initial suite

These evals intentionally start small and focus on stable, high-signal behaviors before adding tool-trajectory checks for live research execution.

## Run from `adk-agent`

Report mode:

```bash
adk eval ./co_scientist tests/eval/evalsets/report_core.json --config_file_path=tests/eval/eval_config.json --print_detailed_results
```

Analysis mode:

```bash
adk eval ./co_scientist_analysis tests/eval/evalsets/analysis_core.json --config_file_path=tests/eval/eval_config.json --print_detailed_results
```

If `adk` is not on your `PATH`, use the virtualenv executable instead:

```bash
./.venv/bin/adk eval ./co_scientist tests/eval/evalsets/report_core.json --config_file_path=tests/eval/eval_config.json --print_detailed_results
```

## Why this starts with semantic evals

- The current report workflow can route between general QA, clarification, and planning without always entering live tool execution.
- Semantic response checks are a better first harness for these paths than brittle tool trajectories.
- The starter config avoids rubric-based judge metrics for callback-driven transfer cases because ADK does not always attach `app_details` metadata for synthetic router transfers, which can cause `NOT_EVALUATED` results even when the behavior is correct.
- Once the mode-specific graphs stabilize further, add targeted tool-trajectory evals for planner/executor behavior and follow-up state handling.
