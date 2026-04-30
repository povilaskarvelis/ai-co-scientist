# AI Co-Scientist Artifacts

This directory defines the repo-native investigation contract for the
agent-native version of AI Co-Scientist.

The current ADK application still exists as the optimized reference workflow.
This artifact contract is the first migration step toward running the same kind
of biomedical investigation directly inside general-purpose coding agents such
as Codex, Cursor, and Claude Code.

## Investigation Layout

Each investigation lives under:

```text
.co-scientist/investigations/<slug>/
```

Required files:

```text
question.md
plan.json
evidence.jsonl
claims.jsonl
report.md
run_notes.md
```

## Lifecycle

1. `question.md` records the original user question and any scope notes.
2. `plan.json` records the ordered evidence-gathering plan before tool calls.
3. `evidence.jsonl` records one evidence object per line as sources are queried.
4. `claims.jsonl` records claim-level conclusions derived from evidence.
5. `report.md` is the final cited Markdown report.
6. `run_notes.md` records provenance, unresolved gaps, and follow-up ideas.

The local files are intended to be readable, diffable, and reusable across agent
sessions. They should contain enough state for another agent session to continue
or edit the investigation without reconstructing everything from chat history.

## Plan Conventions

- `tool_hint` should be a stable machine-actionable tool or dataset identifier,
  not a prose phrase with spaces.
- Put descriptive rationale in `source_notes`.
- Once `report.md` exists, `plan.json` should usually be `synthesized`, and no
  step should still be `pending` or `in_progress`.

## Evidence Conventions

- For structured or quantitative sources, preserve the key machine-reusable
  details in `metadata` in addition to the prose summary.
- Good examples include assay identifiers, potency values, clinical trial phase
  and status, tissue expression values, cell-type labels, and document IDs.

## Schemas

Schemas live in `.co-scientist/schemas/`:

- `plan.schema.json`
- `evidence.schema.json`
- `claims.schema.json`

The schemas define the minimal stable contract. Agents may add fields when a
task needs more structure, but required fields should remain present.

## Validation

Validate a partially completed investigation after planning:

```bash
python scripts/validate_investigation_artifacts.py .co-scientist/investigations/<slug> --stage plan
```

Validate after evidence gathering:

```bash
python scripts/validate_investigation_artifacts.py .co-scientist/investigations/<slug> --stage evidence
```

Validate a completed report:

```bash
python scripts/validate_investigation_artifacts.py .co-scientist/investigations/<slug> --stage report
```

The default stage is `report`.

## Cursor SDK Runner

Cursor integration is generated from the canonical agent-neutral skill:

```bash
python scripts/setup_cursor_integration.py
```

That writes:

```text
.cursor/skills/biomedical-investigation/SKILL.md
.cursor/mcp.json
```

The local Cursor SDK runner lives in `packages/cosci-cursor/`. It asks a Cursor
agent to run the same artifact workflow in stages and validates after each
stage:

```bash
cd packages/cosci-cursor
npm install
export CURSOR_API_KEY=...
npm run start -- run --slug <slug> "<biomedical research question>"
```

The runner defaults to inline MCP configuration so SDK runs receive the
`research-mcp` tools even when ambient Cursor settings are not loaded. Use
`--mcp project` to load `.cursor/mcp.json` through local project settings, or
`--mcp off` for protocol-only smoke tests.

If the project MCP server dependencies are not installed yet, run:

```bash
cd research-mcp
npm install
```

For a no-API smoke test:

```bash
npm run start -- run --dry-run --slug smoke-test "Evaluate EGFR in lung cancer"
```
