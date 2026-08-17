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

  assert.equal(rendered, "Targets identified.");
});

test("handoff removal also clears the executor's dangling transition phrase", () => {
  const rendered = sanitizeDisplaySummary(
    "Two relevant trials were identified. The step is Handoff: NCT06662539 ```json {\"step_id\":\"S5\"}",
  );

  assert.equal(rendered, "Two relevant trials were identified.");
});
