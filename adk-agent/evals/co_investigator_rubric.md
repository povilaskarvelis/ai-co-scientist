# General Co-Investigator Evaluation Rubric (Weighted)

Use this rubric to evaluate co-investigator behavior across a wide range of requests.

## Scoring Method

- Score each criterion on a 0-2 scale:
  - 0 = missing or incorrect
  - 1 = partial
  - 2 = strong
- Weighted score formula:
  - `weighted_points = (raw_score / 2) * weight`
- Total maximum score = 100

## Weighted Criteria

1. **Decomposition Quality** (weight: 12)
   - Plan has 2-4 executable steps.
   - Steps are concrete and aligned with the user objective.

2. **Strategy Fit** (weight: 12)
   - Selected approach matches request type (exploration, comparison, prioritization, validation, action planning).
   - Plan minimizes irrelevant work.

3. **Execution Transparency** (weight: 10)
   - Step-by-step execution log is visible.
   - Status transitions are understandable (`pending/in_progress/completed/blocked`).

4. **Checkpoint Quality (HITL)** (weight: 10)
   - At least one meaningful decision gate is present.
   - Checkpoint choices are actionable (continue/revise/stop equivalent).

5. **Evidence Grounding** (weight: 14)
   - Material claims have traceable references.
   - Evidence supports conclusions rather than being loosely related.

6. **Adaptivity and Fallbacks** (weight: 10)
   - Agent handles tool/data limitations gracefully.
   - It proposes or uses fallback strategies instead of silently failing.

7. **Uncertainty and Risk Handling** (weight: 10)
   - Uncertainty and data gaps are explicit.
   - Risks or caveats are surfaced without overclaiming.

8. **Output Utility** (weight: 10)
   - Final synthesis is decision-ready for the user.
   - Includes clear next actions.

9. **Traceability/Reproducibility** (weight: 6)
   - Another reviewer can follow how conclusions were formed.
   - Key decisions map to steps and evidence.

10. **Efficiency** (weight: 6)
   - Reasonable number of steps/tool calls for the task.
   - Avoids unnecessary loops or repeated low-value actions.

## Thresholds

- **Strong**: >= 80
- **Acceptable**: 65-79
- **Needs Improvement**: < 65

## Failure Signals

- No explicit plan or no checkpoint.
- One-shot narrative with no process trace.
- Claims lack supporting evidence.
- Tool failure is hidden or ignored.
- Output does not provide usable next actions.
