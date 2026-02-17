import re

from run_acceptance_demo import _score_report


TOOL_TRACE_LINE_RE = re.compile(
    r"\d+\.\s+\[(?P<phase>[^\]]+)\]\s+(?P<tool>[a-zA-Z0-9_]+)\(call_id=[^)]*\)\s*->\s*(?P<outcome>[a-z_]+)",
    flags=re.IGNORECASE,
)


def _extract_tool_trace_calls(report: str) -> list[dict]:
    calls: list[dict] = []
    for match in TOOL_TRACE_LINE_RE.finditer(report):
        calls.append(
            {
                "phase": str(match.group("phase")).strip().lower(),
                "tool": str(match.group("tool")).strip(),
                "outcome": str(match.group("outcome")).strip().lower(),
            }
        )
    return calls


def _fallback_sequence_valid(calls: list[dict], primary_tools: set[str], fallback_tools: set[str]) -> bool:
    failure_markers = {"error", "failed", "not_found_or_empty", "degraded", "no_response"}
    fallback_indexes = [idx for idx, call in enumerate(calls) if call["tool"] in fallback_tools]
    if not fallback_indexes:
        return True
    first_fallback = min(fallback_indexes)
    primary_before = [call for idx, call in enumerate(calls) if idx < first_fallback and call["tool"] in primary_tools]
    if not primary_before:
        return False
    return any(call["outcome"] in failure_markers for call in primary_before)


def test_score_report_passes_expected_contract():
    report = """
## Answer
Researchers with strong recent activity include Jane Doe (Example University) and Alex Smith (City Hospital), supported by PMID:12345678 and PMID:23456789.
Main sources informing this answer: OpenAlex, PubMed.

## Rationale
The shortlist emphasizes topic-matched publication activity, affiliation signals, and citation-backed relevance.

## Methodology
The workflow progressed through the following stages: Scope and decomposition -> Evidence collection -> Structured synthesis.
Because this was a researcher-discovery request, the workflow prioritized publication and authorship signals first, then synthesized affiliation and activity evidence.
Evidence was triangulated across OpenAlex, PubMed.
A total of 3 evidence calls were executed: 3 successful and 0 with partial/error outcomes.
No major evidence-retrieval pivots were required.

## Limitations
- Results may omit researchers whose most relevant work is outside indexed sources.

## Next Actions
- Expand the search with adjacent synonyms and validate affiliation recency.

## Decomposition
1. Query disease/topic context and lock timeframe constraints. (completed)
2. Identify topic-matched publications from OpenAlex/PubMed. (completed)
3. Find candidate authors and affiliation signals from publication data. (completed)
4. Assess author activity/prominence and produce a ranked shortlist. (completed)
""".strip()

    checks, metrics = _score_report(report, ["researcher", "affiliation", "pmid", "evidence"])

    assert all(checks.values())
    assert metrics["decomposition_count"] == 4
    assert metrics["tool_call_count"] == 3
    assert metrics["evidence_count"] == 2
    assert metrics["source_family_count"] >= 2
    assert {"openalex", "pubmed"}.issubset(set(metrics["source_families"]))


def test_score_report_fails_on_missing_contract_sections():
    report = "## Answer\nNo structure here.\n"
    checks, metrics = _score_report(report, ["evidence"])

    assert checks["report_contract"] is False
    assert checks["tool_trace_present"] is False
    assert checks["evidence_refs_present"] is False
    assert checks["source_attribution_present"] is False
    assert checks["multi_source_trace"] is False
    assert metrics["decomposition_count"] == 0


def test_score_report_target_assessment_happy_path_contract():
    report = """
## Answer
Recommendation: prioritize LRRK2 over GBA1 with explicit safety monitoring, supported by PMID:34567890 and NCT01234567.
Main sources informing this answer: Open Targets, ClinicalTrials.gov, PubMed.

## Rationale
LRRK2 shows stronger integrated support across genetics, safety, and clinical tractability axes.

## Methodology
The workflow progressed through the following stages: Scope and weighted criteria -> Human genetics evidence -> Safety liabilities and risk signals -> Decision report.
Because risk and safety were central to the question, the workflow emphasized safety and clinical outcome evidence before final recommendation synthesis.
Evidence was triangulated across Open Targets, ClinicalTrials.gov, PubMed.
A total of 5 evidence calls were executed: 5 successful and 0 with partial/error outcomes.
No major evidence-retrieval pivots were required.

## Limitations
- Genetics directionality remains sensitive to cohort selection.

## Next Actions
- Validate the recommendation with one independent evidence refresh.

## Decomposition
1. Lock target pair, disease context, and weighting criteria. (completed)
2. Collect genetics direction and safety evidence first. (completed)
3. Evaluate druggability and competitive development landscape. (completed)
4. Run weighted multi-axis comparison and trade-off analysis. (completed)
""".strip()

    checks, metrics = _score_report(report, ["recommendation", "risk", "safety", "evidence"])

    assert all(checks.values())
    assert metrics["decomposition_count"] == 4
    assert metrics["tool_call_count"] == 5
    assert metrics["source_family_count"] >= 3
    assert {"open_targets", "clinical_trials", "pubmed"}.issubset(set(metrics["source_families"]))


def test_score_report_no_longer_depends_on_diagnostics_or_structured_output():
    report = """
## Answer
Recommendation: provisional due to unresolved risk, with evidence from PMID:45678901 and NCT02345678.
Main sources informing this answer: Open Targets, PubMed.

## Rationale
The recommendation remains provisional because some high-weight evidence remains uncertain.

## Methodology
The workflow progressed through the following stages: Scope -> Evidence retrieval -> Synthesis.
A total of 3 evidence calls were executed: 2 successful and 1 with partial/error outcomes.
When evidence-retrieval issues occurred, the workflow adapted as follows:
- After a degraded response from Open Targets, the workflow pivoted to PubMed.

## Limitations
- High-priority genetics direction evidence remains incomplete.

## Next Actions
- Resolve genetics direction uncertainty with targeted follow-up.

## Decomposition
1. Confirm disease context and comparison scope. (completed)
2. Run genetics direction and safety tools. (completed)
3. Build recommendation with explicit unresolved risk. (completed)
""".strip()

    checks, metrics = _score_report(report, ["risk", "safety", "evidence"])

    assert checks["report_contract"] is True
    assert checks["tool_trace_present"] is True
    assert checks["evidence_refs_present"] is True
    assert "quality_gate_passed" not in checks
    assert "machine_output_contract" not in checks
    assert metrics["tool_call_count"] == 3
    assert metrics["source_family_count"] >= 2


def test_score_report_supports_required_patterns():
    report = """
## Answer
Researcher shortlist includes Jane Doe at Example University with evidence (PMID:12345678).
Main sources informing this answer: OpenAlex, PubMed.

## Rationale
The answer prioritizes publication-backed candidates.

## Methodology
A total of 2 evidence calls were executed: 2 successful and 0 with partial/error outcomes.

## Limitations
- Affiliation snapshots may lag recent moves.

## Next Actions
- Verify institution recency for the top-ranked authors.

## Decomposition
1. Define scope. (completed)
2. Collect evidence. (completed)
""".strip()

    checks, metrics = _score_report(
        report,
        ["researcher", "affiliation", "pmid"],
        required_patterns=[r"\bPMID:\d{5,9}\b", r"\bUniversity\b"],
    )

    assert checks["required_signals_present"] is True
    assert metrics["required_pattern_hits"][r"\bPMID:\d{5,9}\b"] is True
    assert metrics["required_pattern_hits"][r"\bUniversity\b"] is True


def test_score_report_respects_minimum_citations_threshold():
    report = """
## Answer
Recommendation with one citation only (PMID:12345678).
Main sources informing this answer: PubMed, Open Targets.

## Rationale
The decision is preliminary and needs corroboration.

## Methodology
A total of 2 evidence calls were executed: 2 successful and 0 with partial/error outcomes.

## Limitations
- Evidence depth is currently limited.

## Next Actions
- Add one independent confirmatory citation.

## Decomposition
1. Define scope. (completed)
2. Collect evidence. (completed)
""".strip()

    checks, metrics = _score_report(
        report,
        ["recommendation", "evidence"],
        minimum_citations=2,
    )

    assert checks["evidence_refs_present"] is False
    assert metrics["minimum_citations_required"] == 2
    assert metrics["evidence_count"] == 1


def test_report_tool_trace_validates_researcher_fallback_sequence():
    report = """
## Methodology
### Step 2: Evidence collection
- Executed tool trace:
  1. [main] rank_researchers_by_activity(call_id=r1, args={"query":"schizophrenia"}) -> error
  2. [sequence_repair] get_pubmed_author_profile(call_id=r2, args={"authorName":"Jane Doe"}) -> success
  3. [sequence_repair] search_openalex_authors(call_id=r3, args={"query":"schizophrenia"}) -> success
""".strip()

    calls = _extract_tool_trace_calls(report)
    is_valid = _fallback_sequence_valid(
        calls,
        primary_tools={"rank_researchers_by_activity"},
        fallback_tools={"get_pubmed_author_profile", "search_openalex_authors"},
    )

    assert len(calls) == 3
    assert is_valid is True


def test_report_tool_trace_detects_invalid_researcher_fallback_sequence():
    report = """
## Methodology
### Step 2: Evidence collection
- Executed tool trace:
  1. [main] get_pubmed_author_profile(call_id=r4, args={"authorName":"Jane Doe"}) -> success
  2. [main] rank_researchers_by_activity(call_id=r5, args={"query":"schizophrenia"}) -> success
""".strip()

    calls = _extract_tool_trace_calls(report)
    is_valid = _fallback_sequence_valid(
        calls,
        primary_tools={"rank_researchers_by_activity"},
        fallback_tools={"get_pubmed_author_profile", "search_openalex_authors"},
    )

    assert len(calls) == 2
    assert is_valid is False
