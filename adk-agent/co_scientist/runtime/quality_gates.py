"""
Quality gates, adaptive checkpoint policy, and fallback recovery helpers.
"""
from __future__ import annotations

import re


_ASSERTIVE_CLAIM_RE = re.compile(
    r"\b("
    r"recommend(?:ation|ed)?|"
    r"prioriti[sz]e(?:d|s)?|deprioriti[sz]e(?:d|s)?|"
    r"should|must|"
    r"outperform(?:s|ed)?|superior|inferior|"
    r"trap\b"
    r")\b",
    flags=re.IGNORECASE,
)
_HEDGED_CLAIM_RE = re.compile(
    r"\b("
    r"may|might|could|"
    r"possible|possibly|"
    r"preliminary|"
    r"uncertain|unknown|"
    r"insufficient|limited|"
    r"provisional|tentative"
    r")\b",
    flags=re.IGNORECASE,
)


def _contains_pubmed_ref(refs: set[str]) -> bool:
    return any(re.match(r"^PMID:\d{5,9}$", str(ref).strip(), flags=re.IGNORECASE) for ref in refs)


def _select_inline_citation(refs: set[str]) -> str | None:
    if not refs:
        return None
    normalized = sorted({str(ref).strip() for ref in refs if str(ref).strip()})
    priority_prefixes = ("PMID:", "NCT", "DOI:", "OpenAlex:")
    for prefix in priority_prefixes:
        for ref in normalized:
            if ref.upper().startswith(prefix.upper()):
                return ref
    return normalized[0] if normalized else None


def _append_inline_citation_to_first_sentence(text: str, inline_ref: str) -> str:
    body = str(text or "").strip()
    if not body:
        return body
    first_sentence_match = re.match(r"^(.*?[.!?])(\s+.*)?$", body)
    first_sentence = first_sentence_match.group(1).strip() if first_sentence_match else body
    remainder = first_sentence_match.group(2) or "" if first_sentence_match else ""
    if _extract_evidence_refs(first_sentence):
        return body
    if first_sentence.endswith((".", "!", "?")):
        first_sentence = f"{first_sentence[:-1]} ({inline_ref}){first_sentence[-1]}"
    else:
        first_sentence = f"{first_sentence} ({inline_ref})"
    return f"{first_sentence}{remainder}"


def _extract_recommendation_sections(text: str) -> list[str]:
    if not text:
        return []
    lines = str(text).replace("\r\n", "\n").replace("\r", "\n").splitlines()
    inline_label_re = re.compile(
        r"^\s*(?:[-*]\s+)?(?:#+\s*)?(?:\*{0,2})?(?:revised_)?recommendation(?:\*{0,2})\s*:\s*(?:\*{0,2})?\s*(?P<body>.+?)\s*$",
        flags=re.IGNORECASE,
    )
    label_only_re = re.compile(
        r"^\s*(?:[-*]\s+)?(?:#+\s*)?(?:\*{0,2})?(?:revised_)?recommendation(?:\*{0,2})\s*:\s*(?:\*{0,2})?\s*$",
        flags=re.IGNORECASE,
    )
    section_heading_re = re.compile(
        r"^\s*(?:#+\s*)?(?:\*{0,2})?[A-Za-z][A-Za-z0-9 _/\-]{1,60}(?:\*{0,2})?\s*:\s*$",
        flags=re.IGNORECASE,
    )

    extracted: list[str] = []
    idx = 0
    while idx < len(lines):
        line = str(lines[idx]).strip()
        if not label_only_re.match(line):
            inline_match = inline_label_re.match(line)
            if inline_match:
                body = re.sub(r"\s+", " ", inline_match.group("body").strip())
                if body:
                    extracted.append(body)
                idx += 1
                continue
            idx += 1
            continue

        j = idx + 1
        while j < len(lines) and not str(lines[j]).strip():
            j += 1
        section_lines: list[str] = []
        while j < len(lines):
            candidate = str(lines[j]).strip()
            if not candidate:
                if section_lines:
                    break
                j += 1
                continue
            if section_heading_re.match(candidate):
                break
            section_lines.append(candidate)
            j += 1
        if section_lines:
            extracted.append(re.sub(r"\s+", " ", " ".join(section_lines)).strip())
        idx = j

    deduped: list[str] = []
    seen: set[str] = set()
    for section in extracted:
        normalized = section.strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)
    return deduped


def _extract_primary_narrative_paragraph(text: str) -> str:
    if not text:
        return ""
    section_heading_re = re.compile(
        r"^\s*(?:#+\s*)?(?:\*{0,2})?[A-Za-z][A-Za-z0-9 _/\-]{1,60}(?:\*{0,2})?\s*:\s*$",
        flags=re.IGNORECASE,
    )
    for paragraph in re.split(r"\n\s*\n", str(text)):
        normalized = str(paragraph).strip()
        if not normalized:
            continue
        if section_heading_re.match(normalized):
            continue
        return normalized
    return ""


def _inject_inline_citation_in_recommendation(text: str, refs: set[str]) -> str:
    if not text:
        return text
    inline_ref = _select_inline_citation(refs)
    if not inline_ref:
        return text

    lines = text.splitlines()
    updated = False
    for idx, raw in enumerate(lines):
        line = str(raw).rstrip()
        label_only = re.match(
            r"^\s*(?:[-*]\s+)?(?:#+\s*)?(?:\*{0,2})?(?:revised_)?recommendation(?:\*{0,2})\s*:\s*(?:\*{0,2})?\s*$",
            line,
            flags=re.IGNORECASE,
        )
        if label_only:
            target_idx = idx + 1
            while target_idx < len(lines) and not str(lines[target_idx]).strip():
                target_idx += 1
            if target_idx >= len(lines):
                continue
            target_line = str(lines[target_idx]).strip()
            repaired_line = _append_inline_citation_to_first_sentence(target_line, inline_ref)
            if repaired_line == target_line:
                continue
            leading_ws = re.match(r"^\s*", str(lines[target_idx])).group(0)
            lines[target_idx] = f"{leading_ws}{repaired_line}"
            updated = True
            break

        match = re.match(
            r"^(?P<prefix>\s*(?:[-*]\s+)?(?:#+\s*)?(?:\*{0,2})?(?:revised_)?recommendation(?:\*{0,2})\s*:\s*(?:\*{0,2})?\s*)(?P<body>.+)$",
            line,
            flags=re.IGNORECASE,
        )
        if match:
            prefix = match.group("prefix")
            body = match.group("body").strip()
            if not body:
                continue
            repaired = _append_inline_citation_to_first_sentence(body, inline_ref)
            if repaired == body:
                continue
            lines[idx] = f"{prefix}{repaired}"
            updated = True
            break

    if not updated:
        section_heading_re = re.compile(
            r"^\s*(?:#+\s*)?(?:\*{0,2})?[A-Za-z][A-Za-z0-9 _/\-]{1,60}(?:\*{0,2})?\s*:\s*$",
            flags=re.IGNORECASE,
        )
        for idx, raw in enumerate(lines):
            stripped = str(raw).strip()
            if not stripped:
                continue
            if stripped.startswith("#"):
                continue
            if section_heading_re.match(stripped):
                continue
            if re.match(r"^(?:[-*]|\d+[.)])\s+", stripped):
                continue
            if not _ASSERTIVE_CLAIM_RE.search(stripped):
                continue
            repaired = _append_inline_citation_to_first_sentence(stripped, inline_ref)
            if repaired == stripped:
                continue
            leading_ws = re.match(r"^\s*", str(raw)).group(0)
            lines[idx] = f"{leading_ws}{repaired}"
            updated = True
            break

    if not updated:
        return text
    return "\n".join(lines)


def _extract_evidence_refs(text: str) -> set[str]:
    if not text:
        return set()
    pmids = {f"PMID:{match}" for match in re.findall(r"\bPMID[:\s]*([0-9]{5,9})\b", text, flags=re.IGNORECASE)}
    ncts = {match.upper() for match in re.findall(r"\b(NCT[0-9]{8})\b", text, flags=re.IGNORECASE)}
    dois = {f"DOI:{match.lower()}" for match in re.findall(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b", text, flags=re.IGNORECASE)}
    openalex_urls = {match for match in re.findall(r"https?://openalex\.org/[AW]\d+", text, flags=re.IGNORECASE)}
    openalex_ids = {
        f"OpenAlex:{match.upper()}"
        for match in re.findall(r"\bhttps?://openalex\.org/([AW]\d+)\b", text, flags=re.IGNORECASE)
    }
    reactome_ids = {f"Reactome:{match}" for match in re.findall(r"\bR-HSA-\d+\b", text, flags=re.IGNORECASE)}
    string_ids = {f"STRING:{match}" for match in re.findall(r"\b9606\.[A-Za-z0-9_.-]+\b", text, flags=re.IGNORECASE)}
    chembl_ids = {
        f"ChEMBL:{match.upper()}"
        for match in re.findall(r"\b(CHEMBL\d{3,})\b", text, flags=re.IGNORECASE)
    }
    mondo_ids = {
        f"MONDO:{match.upper().replace(':', '_')}"
        for match in re.findall(r"\bMONDO[:_]\d+\b", text, flags=re.IGNORECASE)
    }
    efo_ids = {f"EFO:{match.upper().replace(':', '_')}" for match in re.findall(r"\bEFO[:_]\d+\b", text, flags=re.IGNORECASE)}
    rs_ids = {f"GWAS:{match.lower()}" for match in re.findall(r"\b(rs\d+)\b", text, flags=re.IGNORECASE)}

    return pmids | ncts | dois | openalex_urls | openalex_ids | reactome_ids | string_ids | chembl_ids | mondo_ids | efo_ids | rs_ids


def _collect_trace_entries(task) -> list[dict]:
    entries = [entry for step in task.steps for entry in (step.tool_trace or []) if isinstance(entry, dict)]
    entries.extend(entry for entry in (task.fallback_tool_trace or []) if isinstance(entry, dict))
    return entries


def _collect_output_evidence_refs(task) -> set[str]:
    refs: set[str] = set()
    for step in task.steps:
        refs.update(_extract_evidence_refs(step.output or ""))
    refs.update(_extract_evidence_refs(task.fallback_recovery_notes or ""))
    return refs


def _collect_tool_evidence_refs(trace_entries: list[dict]) -> set[str]:
    refs: set[str] = set()
    for entry in trace_entries:
        for ref in entry.get("evidence_refs") or []:
            normalized = str(ref).strip()
            if normalized:
                refs.add(normalized)
    return refs


def _collect_mcp_contract_violations(trace_entries: list[dict]) -> tuple[int, int, list[str]]:
    expected = 0
    violations: list[str] = []
    for entry in trace_entries:
        outcome = str(entry.get("outcome", "unknown"))
        # Enforce response-contract validation on successful/degraded payloads.
        # Error/empty outcomes may return transport-level failures without a
        # structured payload contract and are handled separately by failure gates.
        if outcome not in {"ok", "degraded"}:
            continue
        expected += 1
        contract = entry.get("response_contract")
        tool_name = str(entry.get("tool_name", "unknown_tool"))
        if not isinstance(contract, dict):
            violations.append(f"{tool_name}: missing response contract metadata.")
            continue
        if not bool(contract.get("valid", False)):
            issues = contract.get("issues")
            issue_text = ""
            if isinstance(issues, list) and issues:
                issue_text = str(issues[0])
            violations.append(f"{tool_name}: {issue_text or 'response contract validation failed.'}")
    return expected, len(violations), violations


def _split_claim_candidates(text: str) -> list[str]:
    if not text:
        return []
    cleaned = str(text).replace("\r\n", "\n").replace("\r", "\n")
    candidates: list[str] = []
    for paragraph in re.split(r"\n\s*\n", cleaned):
        paragraph = paragraph.strip()
        if not paragraph:
            continue
        for raw_segment in re.split(r"(?<=[.!?])\s+|\n+", paragraph):
            segment = str(raw_segment).strip()
            if not segment:
                continue
            segment = re.sub(r"^(?:[-*]|\d+[.)])\s+", "", segment)
            segment = segment.replace("**", "").replace("`", "")
            segment = re.sub(r"\s+", " ", segment).strip()
            if len(segment) < 24:
                continue
            candidates.append(segment)
    return candidates


def _is_assertive_claim(sentence: str) -> bool:
    normalized = str(sentence or "").strip()
    if not normalized:
        return False
    if normalized.startswith("#"):
        return False
    if re.match(r"^(?:recommendation|rationale narrative|next actions?)\s*[:\-]?\s*$", normalized, flags=re.IGNORECASE):
        return False
    if _HEDGED_CLAIM_RE.search(normalized):
        return False
    if not _ASSERTIVE_CLAIM_RE.search(normalized):
        return False
    if normalized.lower().startswith(("next action", "resolve:", "validate the recommendation")):
        return False
    return True


def _collect_uncited_assertive_claims(task, tool_evidence_refs: set[str]) -> tuple[int, list[str]]:
    if not task.steps:
        return 0, []

    final_output = str(task.steps[-1].output or "").strip()
    fallback_text = str(task.fallback_recovery_notes or "").strip()
    synthesis_blocks: list[str] = []

    fallback_recommendations = _extract_recommendation_sections(fallback_text)
    if fallback_recommendations:
        # If fallback includes recommendation-specific sections, only score those sections.
        synthesis_blocks.extend(fallback_recommendations)
    else:
        final_recommendations = _extract_recommendation_sections(final_output)
        if final_recommendations:
            synthesis_blocks.extend(final_recommendations)
        else:
            primary_paragraph = _extract_primary_narrative_paragraph(final_output)
            if primary_paragraph:
                synthesis_blocks.append(primary_paragraph)
    synthesis_text = "\n\n".join(part for part in synthesis_blocks if part)
    if not synthesis_text:
        return 0, []

    uncited_claims: list[str] = []
    assertive_claim_count = 0
    for paragraph in re.split(r"\n\s*\n", synthesis_text):
        paragraph = paragraph.strip()
        if not paragraph:
            continue
        paragraph_refs = _extract_evidence_refs(paragraph)
        for candidate in _split_claim_candidates(paragraph):
            if not _is_assertive_claim(candidate):
                continue
            assertive_claim_count += 1
            candidate_refs = _extract_evidence_refs(candidate)
            if (candidate_refs | paragraph_refs).intersection(tool_evidence_refs):
                continue
            excerpt = candidate if len(candidate) <= 180 else f"{candidate[:177].rstrip()}..."
            if excerpt not in uncited_claims:
                uncited_claims.append(excerpt)
    return assertive_claim_count, uncited_claims


def evaluate_quality_gates(task) -> dict:
    trace_entries = _collect_trace_entries(task)
    tool_evidence_refs = _collect_tool_evidence_refs(trace_entries)
    if task.steps and tool_evidence_refs:
        final_step = task.steps[-1]
        final_output = str(final_step.output or "")
        repaired_final_output = _inject_inline_citation_in_recommendation(final_output, tool_evidence_refs)
        if repaired_final_output != final_output:
            final_step.output = repaired_final_output
        fallback_text = str(task.fallback_recovery_notes or "")
        repaired_fallback_text = _inject_inline_citation_in_recommendation(fallback_text, tool_evidence_refs)
        if repaired_fallback_text != fallback_text:
            task.fallback_recovery_notes = repaired_fallback_text
    output_evidence_refs = _collect_output_evidence_refs(task)
    evidence_count = len(tool_evidence_refs)
    steps_with_output = sum(1 for step in task.steps if step.output and step.output != "(No response generated)")
    coverage_ratio = steps_with_output / len(task.steps) if task.steps else 0.0
    tool_call_count = len(trace_entries)

    unresolved_gaps: list[str] = []

    def _append_gap(message: str) -> None:
        normalized = str(message or "").strip()
        if normalized and normalized not in unresolved_gaps:
            unresolved_gaps.append(normalized)

    combined_output = "\n".join(step.output for step in task.steps if step.output).lower()
    objective_lower = task.objective.lower()
    if "researcher_discovery" in task.intent_tags:
        has_successful_ranking = False
        if "cannot be directly listed" in combined_output or "tool limitation" in combined_output:
            unresolved_gaps.append("Researcher identification appears incomplete due to tool limitations.")
        if not any(token in combined_output for token in ["author", "researcher", "investigator"]):
            unresolved_gaps.append("No explicit researcher entities were reported.")
        researcher_step = next((step for step in task.steps if "evidence" in step.title.lower()), None)
        if researcher_step:
            ranking_calls = [
                entry
                for entry in researcher_step.tool_trace
                if str(entry.get("tool_name", "")) == "rank_researchers_by_activity"
            ]
            successful_ranking = [entry for entry in ranking_calls if str(entry.get("outcome")) == "ok"]
            has_successful_ranking = bool(successful_ranking)
            if not ranking_calls:
                unresolved_gaps.append(
                    "No quantitative ranking tool call (`rank_researchers_by_activity`) was executed."
                )
            elif not successful_ranking:
                unresolved_gaps.append(
                    "No successful quantitative researcher ranking call completed."
                )
            openalex_topic_calls = [
                entry
                for entry in researcher_step.tool_trace
                if str(entry.get("tool_name", "")) in {"rank_researchers_by_activity", "search_openalex_works"}
            ]
            failed_openalex_topic_calls = [
                entry
                for entry in openalex_topic_calls
                if str(entry.get("outcome", "")) in {"error", "no_response"}
            ]
            if openalex_topic_calls and len(failed_openalex_topic_calls) == len(openalex_topic_calls):
                unresolved_gaps.append(
                    "All topic-specific OpenAlex ranking/evidence calls failed; researcher ranking is unreliable."
                )
        top_query = any(
            marker in objective_lower
            for marker in [" top ", "top ", "most active", "prominent", "leading", "most prominent"]
        ) or task.request_type == "prioritization"
        if top_query and not any(
            marker in combined_output for marker in ["activity score", "score:", "ranked", "topic works"]
        ):
            unresolved_gaps.append(
                "Output lacks quantitative ranking metrics for a top/prominent researcher request."
            )
        degraded_researcher_markers = any(
            marker in combined_output
            for marker in [
                "request failed (429)",
                "rate limit",
                "preliminary",
                "could not perform",
                "could not be performed",
            ]
        )
        if degraded_researcher_markers and not (has_successful_ranking and _contains_pubmed_ref(tool_evidence_refs)):
            unresolved_gaps.append(
                "Researcher ranking output still signals degraded evidence quality due to rate limits or incomplete ranking."
            )
        if not _contains_pubmed_ref(tool_evidence_refs):
            _append_gap("Researcher synthesis is missing at least one tool-validated PMID citation from PubMed evidence.")
    if any(token in objective_lower for token in ["target", "druggab", "candidate"]) or "clinical_landscape" in task.intent_tags:
        if any(
            token in combined_output
            for token in [
                "cannot be fulfilled",
                "cannot be completed",
                "insufficient data",
                "no target candidates",
                "unable to identify target",
            ]
        ):
            _append_gap("Target/trial assessment appears incomplete based on model self-reported gaps.")
        if not any(token in combined_output for token in ["ensg", "target id", "candidate target", "phase", "nct"]):
            _append_gap("No concrete target or clinical-trial entities were detected in the synthesis.")

    failed_entries = [
        entry
        for step in task.steps
        for entry in (step.tool_trace or [])
        if str(entry.get("outcome", "")) in {"error", "not_found_or_empty", "no_response", "degraded"}
    ]
    failed_entries.extend(
        entry
        for entry in (task.fallback_tool_trace or [])
        if str(entry.get("outcome", "")) in {"error", "not_found_or_empty", "no_response", "degraded"}
    )
    if failed_entries:
        failure_count = len(failed_entries)
        failure_ratio = failure_count / max(tool_call_count, 1)
        if failure_count >= 2 or failure_ratio >= 0.35:
            _append_gap(
                "Tool execution issues were detected "
                f"({failure_count} failed or empty tool calls)."
            )

    failed_tools = {
        str(entry.get("tool_name", "")).strip()
        for entry in failed_entries
        if str(entry.get("tool_name", "")).strip()
    }
    genetics_priority = (
        "genetics_direction" in task.intent_tags
        or any(token in objective_lower for token in ["genetic", "gwas", "variant", "direction-of-effect"])
    )
    if genetics_priority and failed_tools.intersection(
        {"infer_genetic_effect_direction", "search_gwas_associations", "search_clinvar_variants"}
    ):
        _append_gap("High-priority human genetics direction evidence is incomplete due to tool failures.")
    if "safety_assessment" in task.intent_tags and "summarize_target_safety_liabilities" in failed_tools:
        _append_gap("High-priority safety-liability evidence is incomplete or ambiguous.")

    critical_marker_patterns = (
        r"\bcritical gap\s*[:\-]",
        r"\bcritical missing evidence\b",
        r"\bservice unavailable\b",
        r"\bfailed due to api error(?:s)?\b",
        r"\bcould not retrieve\b",
        r"\bunable to retrieve\b",
        r"\bpersistent failure\b",
    )
    if any(re.search(pattern, combined_output, flags=re.IGNORECASE) for pattern in critical_marker_patterns):
        _append_gap("Output reports critical missing evidence that affects confidence in the recommendation.")
    if any(marker in combined_output for marker in ["not directly from tool output", "historical knowledge"]):
        _append_gap("Synthesis includes claims that are not directly supported by captured tool output.")

    unverified_output_refs = sorted(output_evidence_refs - tool_evidence_refs)
    if unverified_output_refs:
        tolerance = 1 if evidence_count >= 5 else 0
        if len(unverified_output_refs) > tolerance:
            sample = ", ".join(unverified_output_refs[:3])
            _append_gap(
                "Some citation IDs in synthesis are not backed by captured tool responses "
                f"({sample}{', ...' if len(unverified_output_refs) > 3 else ''})."
            )

    assertive_claim_count, uncited_assertive_claims = _collect_uncited_assertive_claims(task, tool_evidence_refs)
    if uncited_assertive_claims:
        sample = "; ".join(uncited_assertive_claims[:2])
        _append_gap(
            "Assertive recommendation claims are missing inline validated citations "
            f"({sample}{'; ...' if len(uncited_assertive_claims) > 2 else ''})."
        )

    if evidence_count == 0:
        _append_gap("No tool-validated citation evidence IDs were captured.")
    if tool_call_count == 0:
        _append_gap("No tool calls were captured for the workflow.")
    executed_tools = {
        str(entry.get("tool_name", "")).strip()
        for entry in trace_entries
        if str(entry.get("tool_name", "")).strip()
    }
    missing_tool_steps = [
        step.title
        for step in task.steps
        if (
            step.status == "completed"
            and step.recommended_tools
            and not step.tool_trace
            and not executed_tools.intersection(
                {str(tool).strip() for tool in step.recommended_tools if str(tool).strip()}
            )
        )
    ]
    if missing_tool_steps:
        _append_gap(
            "Completed steps with recommended tools but no recorded tool execution: "
            + ", ".join(missing_tool_steps)
        )

    contract_expected_count, contract_violation_count, contract_violations = _collect_mcp_contract_violations(
        trace_entries
    )
    if contract_violation_count:
        sample = "; ".join(contract_violations[:3])
        _append_gap(
            "MCP response contract validation failed for one or more tool calls "
            f"({sample}{'; ...' if len(contract_violations) > 3 else ''})."
        )

    passed = (
        evidence_count >= 2
        and coverage_ratio >= 0.9
        and tool_call_count >= 1
        and contract_violation_count == 0
        and len(unresolved_gaps) == 0
    )
    return {
        "passed": passed,
        "evidence_count": evidence_count,
        "tool_evidence_count": len(tool_evidence_refs),
        "output_evidence_count": len(output_evidence_refs),
        "validated_evidence_refs": sorted(tool_evidence_refs),
        "unverified_output_refs": unverified_output_refs,
        "claim_provenance_claim_count": assertive_claim_count,
        "claim_provenance_uncited_count": len(uncited_assertive_claims),
        "claim_provenance_uncited_claims": uncited_assertive_claims,
        "coverage_ratio": coverage_ratio,
        "tool_call_count": tool_call_count,
        "mcp_contract_expected_count": contract_expected_count,
        "mcp_contract_ok_count": max(contract_expected_count - contract_violation_count, 0),
        "mcp_contract_violation_count": contract_violation_count,
        "mcp_contract_violations": contract_violations,
        "unresolved_gaps": unresolved_gaps,
    }


def gate_ack_token(reason: str, plan_version_id: str | None) -> str | None:
    normalized_reason = str(reason or "").strip().lower()
    if not normalized_reason:
        return None
    normalized_plan = str(plan_version_id or "none").strip() or "none"
    return f"gate_ack:{normalized_reason}:{normalized_plan}"


def should_open_checkpoint(
    task,
    next_step,
    quality_state: dict | None = None,
    queued_feedback: list[str] | None = None,
    *,
    active_plan_version_fn=None,
    gate_ack_token_fn=gate_ack_token,
) -> tuple[bool, str]:
    queued = [str(item).strip() for item in (queued_feedback or []) if str(item).strip()]
    if queued:
        return True, "queued_feedback_pending"

    if not next_step:
        return False, "none"

    quality_state = quality_state or {}
    unresolved_gap_count = len(quality_state.get("unresolved_gaps", []) or [])
    last_failures = int(quality_state.get("last_step_failures", 0) or 0)
    last_output = str(quality_state.get("last_step_output", "") or "").lower()
    plan = active_plan_version_fn(task) if active_plan_version_fn else None
    plan_id = plan.version_id if plan else str(task.active_plan_version_id or "none")
    hitl_events = set(task.hitl_history)

    def _is_gate_acknowledged(reason: str) -> bool:
        token = gate_ack_token_fn(reason, plan_id)
        return bool(token and token in hitl_events)

    if next_step.recommended_tools and not any(step.tool_trace for step in task.steps if step.status == "completed"):
        return True, "pre_evidence_execution"

    is_pre_final = bool(task.steps) and next_step.step_id == task.steps[-1].step_id
    if unresolved_gap_count >= 2:
        if _is_gate_acknowledged("quality_gap_spike"):
            return False, "none"
        return True, "quality_gap_spike"
    if is_pre_final and unresolved_gap_count >= 1:
        if _is_gate_acknowledged("quality_gap_spike"):
            return False, "none"
        return True, "quality_gap_spike"

    if last_failures >= 2:
        if _is_gate_acknowledged("repeated_tool_failures"):
            return False, "none"
        return True, "repeated_tool_failures"

    contradiction_markers = (
        "contradict",
        "inconsistent",
        "conflict",
        "uncertain",
        "critical gap",
        "service unavailable",
        "failed due to api error",
        "failed due to api errors",
        "could not retrieve",
        "unable to retrieve",
    )
    if any(marker in last_output for marker in contradiction_markers):
        if _is_gate_acknowledged("uncertainty_spike"):
            return False, "none"
        return True, "uncertainty_spike"

    if is_pre_final and any(event.lower().startswith("revise:") for event in task.hitl_history):
        if _is_gate_acknowledged("pre_final_after_intent_change"):
            return False, "none"
        return True, "pre_final_after_intent_change"

    return False, "none"


def render_quality_gate_message(report: dict) -> str:
    lines = [
        "[Quality Gate Check]",
        f"- Evidence references found: {report['evidence_count']}",
        f"- Output references detected: {report.get('output_evidence_count', 0)}",
        f"- Step coverage ratio: {report['coverage_ratio']:.2f}",
        f"- Tool calls captured: {report.get('tool_call_count', 0)}",
        (
            "- MCP response contracts: "
            f"{report.get('mcp_contract_ok_count', 0)}/{report.get('mcp_contract_expected_count', 0)} valid"
        ),
        (
            "- Claim-level provenance: "
            f"{report.get('claim_provenance_claim_count', 0)} assertive claims scanned, "
            f"{report.get('claim_provenance_uncited_count', 0)} missing inline validated citations"
        ),
    ]
    if report["unresolved_gaps"]:
        lines.append("- Unresolved critical gaps:")
        lines.extend([f"  - {gap}" for gap in report["unresolved_gaps"]])
    else:
        lines.append("- Unresolved critical gaps: none")
    return "\n".join(lines)


def clean_recovery_text(text: str) -> str:
    if not text:
        return text
    seen = set()
    cleaned_lines: list[str] = []
    for raw in text.splitlines():
        line = raw.rstrip()
        normalized = re.sub(r"\s+", " ", line.strip().lower())
        if normalized in {"**3. key results:**", "3. key results:"}:
            if "3-key-results" in seen:
                continue
            seen.add("3-key-results")
        if normalized and normalized in seen and normalized.startswith("**"):
            continue
        if normalized:
            seen.add(normalized)
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines).strip()


async def run_fallback_recovery(
    runner,
    session_id: str,
    user_id: str,
    task,
    *,
    run_runner_turn_with_trace_fn,
    format_step_execution_error_fn,
    clean_recovery_text_fn=clean_recovery_text,
) -> tuple[str, list[dict]]:
    fallback_tools: list[str] = []
    for step in task.steps:
        fallback_tools.extend(step.fallback_tools)
    fallback_tools = sorted(set(fallback_tools))
    trace_entries = _collect_trace_entries(task)
    tool_evidence_refs = _collect_tool_evidence_refs(trace_entries)
    executed_tools = {
        str(entry.get("tool_name", "")).strip()
        for entry in trace_entries
        if str(entry.get("tool_name", "")).strip()
    }
    missing_recommended_tools: list[str] = []
    for step in task.steps:
        if step.status != "completed" or not step.recommended_tools or step.tool_trace:
            continue
        for tool in step.recommended_tools:
            normalized_tool = str(tool).strip()
            if not normalized_tool or normalized_tool in executed_tools:
                continue
            if normalized_tool not in missing_recommended_tools:
                missing_recommended_tools.append(normalized_tool)

    fallback_guidance = ""
    if "researcher_discovery" in task.intent_tags:
        fallback_guidance = (
            "For researcher ranking requests, prioritize publication-centric recovery tools in this order: "
            "rank_researchers_by_activity, search_openalex_works, search_pubmed_advanced, get_pubmed_author_profile. "
            "Avoid clinical-trials-only fallback unless explicitly requested.\n"
        )
        if not _contains_pubmed_ref(tool_evidence_refs):
            fallback_guidance += (
                "Mandatory recovery action: execute `search_pubmed_advanced` and include at least one PMID:#### "
                "citation in revised_recommendation or key_results.\n"
            )
    if missing_recommended_tools:
        fallback_guidance += (
            "Mandatory recovery action: execute at least one previously-missed recommended tool if feasible: "
            f"{', '.join(missing_recommended_tools)}.\n"
        )
    prompt = (
        "Perform one fallback recovery pass before final synthesis.\n"
        f"Objective: {task.objective}\n"
        f"Intent tags: {', '.join(task.intent_tags)}\n"
        f"Fallback tools to prioritize: {', '.join(fallback_tools) if fallback_tools else 'N/A'}\n"
        f"{fallback_guidance}"
        "You must execute at least one relevant tool call unless no relevant tool exists.\n"
        "Required output fields: selected_tools, why_chosen, revised_recommendation, key_results, remaining_gaps.\n"
        "For revised_recommendation, the first sentence must include at least one inline citation ID "
        "(PMID/NCT/DOI/OpenAlex) when any validated evidence refs are available.\n"
        "Use explicit citations where possible."
    )
    try:
        raw, trace_entries = await run_runner_turn_with_trace_fn(runner, session_id, user_id, prompt)
    except Exception as exc:
        return format_step_execution_error_fn(exc, fallback_tools), []
    for entry in trace_entries:
        entry["phase"] = "fallback_recovery"

    need_pubmed_repair = (
        "researcher_discovery" in task.intent_tags
        and not _contains_pubmed_ref(tool_evidence_refs)
        and not any(
            str(entry.get("tool_name", "")) == "search_pubmed_advanced"
            and str(entry.get("outcome", "")) == "ok"
            for entry in trace_entries
        )
    )
    if need_pubmed_repair:
        repair_prompt = (
            "Fallback repair required: prior recovery did not run `search_pubmed_advanced` successfully.\n"
            f"Objective: {task.objective}\n"
            "Execute `search_pubmed_advanced` now with the same scoped topic/timeframe and return:\n"
            "- selected_tools\n"
            "- revised_recommendation (first sentence must include PMID citation)\n"
            "- key_results\n"
            "- remaining_gaps\n"
            "Use only citation IDs directly supported by this run."
        )
        try:
            repaired_raw, repair_trace = await run_runner_turn_with_trace_fn(runner, session_id, user_id, repair_prompt)
        except Exception:
            repair_trace = []
            repaired_raw = ""
        if repair_trace:
            for entry in repair_trace:
                entry["phase"] = "fallback_recovery_repair"
            trace_entries.extend(repair_trace)
            if repaired_raw.strip():
                raw = repaired_raw

    cleaned = clean_recovery_text_fn(raw)
    recovery_refs = _collect_tool_evidence_refs(trace_entries)
    augmented_refs = set(tool_evidence_refs).union(recovery_refs)
    repaired = _inject_inline_citation_in_recommendation(cleaned, augmented_refs)
    return repaired, trace_entries


async def complete_remaining_steps(
    runner,
    session_id: str,
    user_id: str,
    task,
    state_store,
    *,
    execute_step_fn,
    evaluate_quality_gates_fn=evaluate_quality_gates,
    render_quality_gate_message_fn=render_quality_gate_message,
    run_fallback_recovery_fn=None,
    print_fn=print,
) -> dict:
    if run_fallback_recovery_fn is None:
        raise ValueError("run_fallback_recovery_fn is required")

    task.fallback_recovery_notes = ""
    task.fallback_tool_trace = []
    for idx in range(task.current_step_index + 1, len(task.steps)):
        step_text = await execute_step_fn(runner, session_id, user_id, task, idx)
        state_store.save_task(task, note=f"step_{idx + 1}_completed")
        print_fn(step_text)

    quality = evaluate_quality_gates_fn(task)
    print_fn("\n" + render_quality_gate_message_fn(quality))
    if not quality["passed"]:
        print_fn("\nRunning one fallback recovery pass...")
        recovery, recovery_trace = await run_fallback_recovery_fn(runner, session_id, user_id, task)
        task.fallback_tool_trace = recovery_trace
        task.fallback_recovery_notes = recovery or ""
        quality = evaluate_quality_gates_fn(task)
    return quality


def format_checkpoint_reason(reason: str) -> str:
    mapping = {
        "pre_evidence_execution": "Before bulk evidence collection",
        "quality_gap_spike": "Quality/uncertainty spike detected",
        "repeated_tool_failures": "Repeated tool failures detected",
        "uncertainty_spike": "Uncertainty spike detected",
        "pre_final_after_intent_change": "Intent changed before final synthesis",
        "feedback_replan": "Plan updated from user feedback",
        "queued_feedback_pending": "Queued feedback pending application",
    }
    key = str(reason or "").strip()
    return mapping.get(key, key.replace("_", " ") if key else "unspecified")


def print_checkpoint_plan(
    task,
    *,
    active_plan_version_fn,
    format_checkpoint_reason_fn=format_checkpoint_reason,
    print_fn=print,
) -> None:
    print_fn("\n[Checkpoint Plan]")
    if task.latest_plan_delta:
        delta = task.latest_plan_delta
        print_fn("What changed:")
        print_fn(f"- {delta.summary or 'No structural changes.'}")
        if delta.added_steps:
            print_fn(f"- Added: {', '.join(delta.added_steps)}")
        if delta.removed_steps:
            print_fn(f"- Removed: {', '.join(delta.removed_steps)}")
        if delta.modified_steps:
            print_fn(f"- Modified: {', '.join(delta.modified_steps)}")
        if delta.reordered_steps:
            print_fn(f"- Reordered: {', '.join(delta.reordered_steps)}")
        print_fn("")

    version = active_plan_version_fn(task)
    if version and version.steps:
        print_fn("Remaining plan:")
        for idx, step in enumerate(version.steps, start=1):
            print_fn(f"{idx}. {step.title}")
    else:
        print_fn("Remaining plan: none")

    if task.checkpoint_reason:
        print_fn(f"\nCheckpoint reason: {format_checkpoint_reason_fn(task.checkpoint_reason)}")
