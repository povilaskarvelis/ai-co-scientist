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
    return String(text || "")
      .replace(/\bHand-?off:\s*`{2,3}(?:json)?[\s\S]*?`{2,3}/gi, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  return { sanitizeDisplaySummary, shouldUseStartingPlaceholder };
}));
