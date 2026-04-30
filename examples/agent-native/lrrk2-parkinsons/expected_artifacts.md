# Expected Artifacts

This example is the first walking-skeleton test for the agent-native packaging.

After an agent runs the investigation, this folder should exist:

```text
.co-scientist/investigations/lrrk2-parkinsons/
```

It should contain:

```text
question.md
plan.json
evidence.jsonl
claims.jsonl
report.md
run_notes.md
```

Run validation as the investigation progresses:

```bash
python scripts/validate_investigation_artifacts.py .co-scientist/investigations/lrrk2-parkinsons --stage plan
python scripts/validate_investigation_artifacts.py .co-scientist/investigations/lrrk2-parkinsons --stage evidence
python scripts/validate_investigation_artifacts.py .co-scientist/investigations/lrrk2-parkinsons --stage report
```

The report-stage validation should pass only when the final report and claim
records exist and at least one source identifier is preserved.

The key follow-up editability test is:

> Add a table comparing genetics, expression, clinical, and druggability evidence
> without rerunning the full pipeline unless the existing artifacts lack the
> necessary evidence.
