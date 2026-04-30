# AI Co-Scientist Agent Instructions

This repository is experimenting with an agent-native packaging of AI Co-Scientist.
The existing ADK application remains the reference implementation, but coding agents
should be able to run a repo-native biomedical investigation by writing local
artifacts instead of relying on hidden session state.

## Biomedical Research Workflow

For complex biomedical research questions, use the
`.agents/skills/biomedical-investigation/SKILL.md` workflow.

Complex questions include target validation, therapeutic comparison, clinical or
safety evidence review, variant interpretation, dataset discovery, translational
biology, and any request that needs cited evidence from biomedical databases.

## Artifact Contract

Every investigation must write state under:

```text
.co-scientist/investigations/<slug>/
```

Use this file layout:

```text
question.md
plan.json
evidence.jsonl
claims.jsonl
report.md
run_notes.md
```

Do not keep the only copy of the plan, evidence, claims, or report in chat. The
local artifacts are the source of truth so future agent sessions can inspect,
edit, validate, and extend the work.

## Source Grounding

- Prefer the repo's biomedical MCP tools in `research-mcp/` before broad web
  search when a relevant source tool exists.
- Use stable machine-actionable tool identifiers in plans, such as
  `search_pubmed`, `get_clinical_trial`, `get_gene_tissue_expression`, or
  `get_chembl_bioactivities`. Put any human-readable nuance in a separate
  `source_notes` field instead of embedding prose into `tool_hint`.
- Preserve concrete source identifiers for substantive claims whenever available:
  `PMID:...`, `DOI:...`, `NCT...`, `UniProt:...`, `PubChem:...`, `CHEMBL...`,
  `PDB:...`, `rs...`, `GCST...`, and source-specific accessions.
- Attach identifiers to the specific claim they support.
- Separate evidence strength from source count. Curated clinical, regulatory,
  genetic, experimental, aggregate, and metadata-only sources should not be
  treated as interchangeable.
- When evidence comes from a structured or quantitative source, preserve the key
  numeric details in `metadata` as well as the prose summary.

## Validation

Use the artifact validator as the workflow progresses:

```bash
python scripts/validate_investigation_artifacts.py .co-scientist/investigations/<slug> --stage plan
python scripts/validate_investigation_artifacts.py .co-scientist/investigations/<slug> --stage evidence
python scripts/validate_investigation_artifacts.py .co-scientist/investigations/<slug> --stage report
```

The default validation stage is `report`.
