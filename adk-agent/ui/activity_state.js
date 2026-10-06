(function attachActivityStateHelpers(root, factory) {
  const helpers = factory();
  if (typeof module !== "undefined" && module.exports) {
    module.exports = helpers;
  }
  root.CoScientistActivityState = helpers;
}(typeof globalThis !== "undefined" ? globalThis : this, function createActivityStateHelpers() {
  const ACTIVE_RUN_STATUSES = new Set(["running", "queued", "in_progress"]);

  function shouldUseStartingPlaceholder(isStarting, runStatus) {
    return Boolean(isStarting) && !ACTIVE_RUN_STATUSES.has(String(runStatus || "").trim());
  }

  function sanitizeDisplaySummary(text) {
    const visibleText = String(text || "")
      .split(/\bHand-?off:/i)[0]
      .split(/(?:^|\s)#{1,6}\s*Completed\s+step\s+S?\d+\b/i)[0]
      .split(/\{\s*"(?:schema|step_id|result_summary|structured_observations|handoff)"\s*:/i)[0];
    return visibleText
      .replace(/`{2,3}\s*(?:json)?\s*\{[\s\S]*$/i, " ")
      .replace(/(?:^|\s)#{1,6}\s+/g, " ")
      .replace(/`{2,3}\s*(?:json)?/gi, " ")
      .replace(/\bThe step is\s*$/i, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function calculatePreservedScrollTop({
    previousScrollTop = 0,
    previousScrollHeight = 0,
    clientHeight = 0,
    nextScrollHeight = 0,
    bottomThreshold = 24,
  } = {}) {
    const viewportHeight = Math.max(0, Number(clientHeight) || 0);
    const previousMax = Math.max(0, (Number(previousScrollHeight) || 0) - viewportHeight);
    const nextMax = Math.max(0, (Number(nextScrollHeight) || 0) - viewportHeight);
    const previousTop = Math.max(0, Math.min(Number(previousScrollTop) || 0, previousMax));
    const wasNearBottom = previousMax - previousTop <= Math.max(0, Number(bottomThreshold) || 0);
    return wasNearBottom ? nextMax : Math.min(previousTop, nextMax);
  }

  // Planning has no streamed progress, so show what is happening in stages while it runs.
  function planningStageLabel(elapsedMs) {
    const seconds = Math.max(0, Number(elapsedMs) || 0) / 1000;
    if (seconds < 4) return "Reading your question";
    if (seconds < 10) return "Choosing which databases to search";
    return "Drafting a research plan";
  }

  function formatElapsed(ms) {
    const total = Math.max(0, Math.round((Number(ms) || 0) / 1000));
    const minutes = Math.floor(total / 60);
    const seconds = String(total % 60).padStart(2, "0");
    return `${minutes}:${seconds}`;
  }

  // One short line for a finished step: drop leaked section headings and keep the first sentence.
  function cleanStepFinding(text, maxChars = 170) {
    let value = sanitizeDisplaySummary(text)
      .replace(/\*\*/g, "")
      .replace(/^(?:key |detailed )?findings(?: summary)?\s*:?\s*/i, "")
      .replace(/^summary\s*:?\s*/i, "")
      .trim();
    // A raw payload (an executor hand-off list) is not a finding; show nothing rather than JSON.
    if (!value || /^[[{]/.test(value)) return "";
    const firstSentence = value.match(/^(.{40,}?[.!?])(?:\s|$)/);
    if (firstSentence) value = firstSentence[1];
    return value.length > maxChars ? `${value.slice(0, maxChars - 1).trimEnd()}…` : value;
  }

  function lastToolSummary(detail) {
    const log = Array.isArray(detail?.tool_log) ? detail.tool_log : [];
    for (let idx = log.length - 1; idx >= 0; idx -= 1) {
      const summary = String(log[idx]?.summary || "").trim();
      if (summary) return summary;
    }
    return "";
  }

  // Merge the plan's steps with live run data (or the saved research log) into what the plan
  // checklist renders: a phase for the whole run plus a status and one line per step.
  function computePlanProgress({
    runStatus = "",
    taskStatus = "",
    awaitingApproval = false,
    started = false,
    events = [],
    summaries = [],
    steps = [],
  } = {}) {
    const run = String(runStatus || "").trim();
    const task = String(taskStatus || "").trim();
    const isActive = ACTIVE_RUN_STATUSES.has(run);
    const safeEvents = Array.isArray(events) ? events : [];
    const safeSummaries = Array.isArray(summaries) ? summaries : [];
    const latestSummary = safeSummaries.length ? safeSummaries[safeSummaries.length - 1] : null;
    const details = Array.isArray(latestSummary?.step_details) ? latestSummary.step_details : [];
    const detailById = new Map(details.map((detail) => [String(detail?.id || ""), detail]));

    const liveLineById = new Map();
    let currentStepId = "";
    let startedAt = "";
    let finishedAt = "";
    for (const event of safeEvents) {
      const type = String(event?.type || "");
      const stepId = String(event?.metrics?.step_id || "").trim();
      if ((type === "execution.running" || type === "step.started") && !startedAt) {
        startedAt = String(event?.at || "");
      }
      if (type === "step.started" && stepId) currentStepId = stepId;
      if (type === "tool.called") {
        const target = stepId || currentStepId;
        const line = String(event?.human_line || "").trim();
        if (target && line) liveLineById.set(target, line);
      }
      if (type === "run.completed" || type === "run.failed" || type === "run.interrupted") {
        finishedAt = String(event?.at || "");
      }
    }

    let phase = "awaiting";
    if (run === "failed" || task === "failed") phase = "failed";
    else if (isActive) phase = "running";
    else if (task === "completed") phase = "done";
    else if (started && !awaitingApproval) phase = "running";

    const stepStates = {};
    let finishedCount = 0;
    let runningId = "";
    for (const step of Array.isArray(steps) ? steps : []) {
      const stepId = String(step?.id || "").trim();
      if (!stepId) continue;
      const detail = detailById.get(stepId) || {};
      let status = phase === "awaiting" ? "pending" : String(detail.status || step?.status || "pending").trim();
      if (!["pending", "in_progress", "completed", "blocked"].includes(status)) status = "pending";
      let line = "";
      if (status === "in_progress") {
        runningId = runningId || stepId;
        line = liveLineById.get(stepId) || lastToolSummary(detail) || "Working…";
      } else if (status === "completed" || status === "blocked") {
        finishedCount += 1;
        line = cleanStepFinding(
          detail.step_progress_note || detail.result_summary || step?.step_progress_note || step?.result_summary || "",
        );
      }
      stepStates[stepId] = { status, line };
    }

    const stepIds = Object.keys(stepStates);
    if (phase === "running" && stepIds.length && finishedCount === stepIds.length) {
      phase = "writing";
    } else if (phase === "running" && !runningId) {
      // The run has started but the first tool call has not arrived yet.
      const nextId = stepIds.find((stepId) => stepStates[stepId].status === "pending");
      if (nextId) stepStates[nextId] = { status: "in_progress", line: liveLineById.get(nextId) || "Starting…" };
    }
    return { phase, startedAt, finishedAt, stepStates };
  }

  function planStatusText(progress, nowMs = Date.now(), fallbackStartedAt = "") {
    const phase = String(progress?.phase || "");
    const startMs = Date.parse(progress?.startedAt || fallbackStartedAt || "");
    const elapsed = Number.isFinite(startMs) ? formatElapsed(nowMs - startMs) : "";
    if (phase === "running") return `Researching${elapsed ? ` · ${elapsed}` : ""} · usually 2–5 min`;
    if (phase === "writing") return `Writing the report${elapsed ? ` · ${elapsed}` : ""}`;
    if (phase === "done") {
      const endMs = Date.parse(progress?.finishedAt || "");
      return Number.isFinite(startMs) && Number.isFinite(endMs) ? `Done in ${formatElapsed(endMs - startMs)}` : "Done";
    }
    if (phase === "failed") return "Stopped";
    return "";
  }

  return {
    calculatePreservedScrollTop,
    cleanStepFinding,
    computePlanProgress,
    formatElapsed,
    planStatusText,
    planningStageLabel,
    sanitizeDisplaySummary,
    shouldUseStartingPlaceholder,
  };
}));
