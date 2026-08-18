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

  return {
    calculatePreservedScrollTop,
    sanitizeDisplaySummary,
    shouldUseStartingPlaceholder,
  };
}));
