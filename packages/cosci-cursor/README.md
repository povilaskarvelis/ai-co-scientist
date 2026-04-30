# AI Co-Scientist Cursor Runner

This package is the first Cursor SDK harness for the repo-native AI
Co-Scientist workflow. It keeps the biomedical workflow agent-native by asking a
local Cursor agent to write files under `.co-scientist/investigations/<slug>/`,
then gates each phase with the Python artifact validator.

## Setup

From the repository root:

```bash
python scripts/setup_cursor_integration.py
cd research-mcp
npm install
cd packages/cosci-cursor
npm install
```

Set a Cursor API key before running the agent:

```bash
export CURSOR_API_KEY=...
```

## Run

```bash
npm run start -- run \
  --slug lrrk2-parkinsons \
  "Evaluate LRRK2 as a therapeutic target in Parkinson disease"
```

By default, the runner passes `research-mcp/server.js` to the Cursor SDK as an
inline stdio MCP server. This is deliberate: local SDK agents do not load
`.cursor/mcp.json` unless project settings are explicitly enabled.

The runner executes these gated stages:

1. `plan`: write `question.md` and `plan.json`
2. `evidence`: gather evidence and write `evidence.jsonl`
3. `report`: write `claims.jsonl`, `report.md`, and `run_notes.md`

After each stage it runs:

```bash
python scripts/validate_investigation_artifacts.py .co-scientist/investigations/<slug> --stage <stage>
```

If validation fails, the runner sends the validator output back to the Cursor
agent for repair. The default is two repair attempts per stage.

## Useful Commands

Preview prompts without calling Cursor:

```bash
npm run start -- run --dry-run --slug smoke-test "Evaluate EGFR in lung cancer"
```

Validate an existing investigation through the runner:

```bash
npm run start -- validate lrrk2-parkinsons --stage report
```

Choose a model:

```bash
npm run start -- run --model composer-2 "Compare JAK inhibitors for thrombotic risk"
```

Use project Cursor settings instead of inline MCP:

```bash
npm run start -- run --mcp project "Evaluate EGFR in lung cancer"
```

Disable MCP for a pure artifact/protocol smoke test:

```bash
npm run start -- run --mcp off "Evaluate EGFR in lung cancer"
```

Resume an existing local Cursor agent:

```bash
npm run start -- run --agent-id <agent-id> "Continue the same investigation"
```

The runner writes `cursor-run.jsonl` and `cursor-run-manifest.json` into the
investigation folder for local provenance.
