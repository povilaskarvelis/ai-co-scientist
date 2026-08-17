const test = require("node:test");
const assert = require("node:assert/strict");

const { sanitizeDisplaySummary, shouldUseStartingPlaceholder } = require("./activity_state.js");

test("temporary execution placeholder yields to a live run snapshot", () => {
  assert.equal(shouldUseStartingPlaceholder(true, ""), true);
  assert.equal(shouldUseStartingPlaceholder(true, "awaiting_hitl"), true);
  assert.equal(shouldUseStartingPlaceholder(true, "queued"), false);
  assert.equal(shouldUseStartingPlaceholder(true, "running"), false);
  assert.equal(shouldUseStartingPlaceholder(true, "in_progress"), false);
  assert.equal(shouldUseStartingPlaceholder(false, "running"), false);
});

test("structured handoff payloads stay out of the human activity summary", () => {
  const rendered = sanitizeDisplaySummary(
    "Targets identified. Hand-off: ``json [{\"entity_type\":\"protein\",\"label\":\"GFRAL\"}] `` Used curated sources.",
  );

  assert.equal(rendered, "Targets identified. Used curated sources.");
});
