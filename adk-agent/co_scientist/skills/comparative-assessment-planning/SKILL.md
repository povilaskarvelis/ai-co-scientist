---
name: comparative-assessment-planning
description: Use when planning head-to-head comparisons across drugs, targets, variants, datasets, or interventions where evidence should be aligned by shared dimensions.
---

Use this skill when the objective explicitly compares two or more entities, or when the answer depends on choosing between alternatives.

Instructions:
1. Plan comparisons by shared evidence dimension and source, not by entity order. Typical dimensions include efficacy, safety, mechanism, tractability, selectivity, biomarker context, trial activity, and evidence quality.
2. Apply each evidence step across all compared entities so the executor can collect comparable fields with multiple source calls inside that step. Do not create parallel copies of the same source step for entity A, entity B, and so on.
3. Put the requested overall lean, recommendation, or ranking in the plan's success criteria. The downstream report synthesizer performs the arbitration; do not add a synthesis-only plan step.
4. If evidence coverage is asymmetric, plan to state that asymmetry explicitly rather than pretending the comparison is balanced.
5. Load `references/comparative-assessment-playbook.md` before finalizing comparative plans.
