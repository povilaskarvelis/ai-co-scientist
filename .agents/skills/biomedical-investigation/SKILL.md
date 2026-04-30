---
name: biomedical-investigation
description: Use for complex biomedical research questions that need local planning, tool-backed evidence gathering, claim extraction, and a cited Markdown report.
---

# Biomedical Investigation

Use this skill when a user asks a complex biomedical question that should be
answered with cited evidence rather than only general knowledge. Examples include
therapeutic target validation, clinical/safety comparison, variant interpretation,
dataset discovery, translational biology, and mechanism-focused evidence reviews.

## Workflow

1. Create an investigation folder at `.co-scientist/investigations/<slug>/`.
   Use a short lowercase slug derived from the question.
2. Write the original user question to `question.md`.
3. Write `plan.json` before gathering evidence.
4. Execute one plan step at a time. Prefer relevant biomedical MCP tools from
   `research-mcp/` before broad web search.
5. Append evidence records to `evidence.jsonl` as evidence is collected.
6. Extract claim-level records into `claims.jsonl`.
7. Write the final cited Markdown report to `report.md`.
8. Write brief provenance and follow-up notes to `run_notes.md`.
9. Validate artifacts with `scripts/validate_investigation_artifacts.py`.

The local files are the source of truth. Do not rely on chat history as the only
place where the plan, evidence, or conclusions exist.

## Required Files

```text
.co-scientist/investigations/<slug>/
  question.md
  plan.json
  evidence.jsonl
  claims.jsonl
  report.md
  run_notes.md
```

## Plan Rules

`plan.json` must use schema `co_scientist.plan.v1`.

The plan must include:

- `objective`: a clear restatement of the research question.
- `success_criteria`: concrete checks for a useful answer.
- `steps`: ordered atomic evidence-gathering steps.

Each step must include:

- `id`: `S1`, `S2`, `S3`, etc.
- `goal`: the evidence question for this step.
- `tool_hint`: the preferred stable tool or dataset identifier, such as
  `search_pubmed`, `get_clinical_trial`, `get_gene_tissue_expression`, or
  `get_chembl_bioactivities`.
- `source_notes`: optional human-readable nuance about source choice.
- `domains`: 1-3 biomedical domains.
- `completion_condition`: how the step knows it has enough evidence.
- `status`: `pending`, `in_progress`, `completed`, `blocked`, or `skipped`.

Start with high-signal structured or curated sources when the question calls for
quantitative, clinical, target-validation, or identifier-specific evidence. Add
literature grounding when structured sources do not return PMIDs, DOIs, NCT IDs,
or equivalent identifiers.

## Evidence Rules

Append one JSON object per line to `evidence.jsonl` using schema
`co_scientist.evidence.v1`.

Each evidence record should capture:

- `id`: `E1`, `E2`, `E3`, etc.
- `step_id`: the plan step this evidence supports.
- `source`: human-readable source name, such as `PubMed` or `ClinicalTrials.gov`.
- `tool`: tool name or lookup method used.
- `claim`: the atomic claim supported or challenged.
- `summary`: compact details, including numbers, entities, and caveats.
- `identifiers`: concrete source identifiers when available.
- `evidence_type`: source family, such as `literature`, `clinical_trial`,
  `structured_data`, `database_record`, `regulatory`, `experimental`,
  `metadata`, or `other`.
- `confidence`: `high`, `moderate`, `low`, `mixed`, or `unknown`.
- `limitations`: source-specific caveats.
- `metadata`: structured quantitative details when available, such as potency
  values, assay IDs, phase/status fields, tissue TPM values, cell types, cohort
  names, or other machine-reusable observations.

Do not fabricate identifiers. If a source does not provide identifiers, state the
limitation and add a grounded follow-up lookup when it materially improves the
answer.

## Claim Rules

Append one JSON object per line to `claims.jsonl` using schema
`co_scientist.claim.v1`.

Each claim should:

- state one substantive conclusion,
- cite supporting evidence record IDs,
- preserve source identifiers,
- assign a confidence label,
- mark the claim status as `supported`, `mixed`, `unsupported`, or
  `needs_review`.

Prefer claim-local citations over pooled bibliography lists.

## Report Rules

`report.md` should be user-facing Markdown. Use the local evidence and claims as
the authority; do not invent claims during synthesis.

Recommended sections:

- `## TLDR`
- `## Evidence Breakdown`
- `## Conflicting & Uncertain Evidence` when needed
- `## Limitations`
- `## Recommended Next Steps`

Use readable source names, not raw tool names. Include identifiers inline near
the claims they support.

## Follow-Up Editing

After a report exists, the user may ask for tables, restructuring, LaTeX export,
additional analysis, or a narrowed follow-up. Prefer editing or extending the
local artifacts directly. Only rerun evidence collection when the requested
change needs new evidence.

## Validation Commands

```bash
python scripts/validate_investigation_artifacts.py .co-scientist/investigations/<slug> --stage plan
python scripts/validate_investigation_artifacts.py .co-scientist/investigations/<slug> --stage evidence
python scripts/validate_investigation_artifacts.py .co-scientist/investigations/<slug> --stage report
```
