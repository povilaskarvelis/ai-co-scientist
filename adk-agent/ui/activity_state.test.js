const test = require("node:test");
const assert = require("node:assert/strict");

const {
  calculatePreservedScrollTop,
  sanitizeDisplaySummary,
  shouldUseStartingPlaceholder,
} = require("./activity_state.js");

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

test("legacy completion payloads and Markdown headings stay out of activity summaries", () => {
  const rendered = sanitizeDisplaySummary(
    "## S1 Canonical identifiers were resolved. ### Resolved identifiers CALCR: ENSG00000004948. "
      + "## Completed step S1 ``json {\"step_id\":\"S1\",\"handoff\":{}} ``",
  );

  assert.equal(
    rendered,
    "S1 Canonical identifiers were resolved. Resolved identifiers CALCR: ENSG00000004948.",
  );
});

test("activity updates preserve a reader's position away from the bottom", () => {
  assert.equal(calculatePreservedScrollTop({
    previousScrollTop: 180,
    previousScrollHeight: 900,
    clientHeight: 380,
    nextScrollHeight: 980,
  }), 180);
});

test("activity updates continue following entries when already at the bottom", () => {
  assert.equal(calculatePreservedScrollTop({
    previousScrollTop: 515,
    previousScrollHeight: 900,
    clientHeight: 380,
    nextScrollHeight: 980,
  }), 600);
});

test("activity updates clamp the preserved position when content becomes shorter", () => {
  assert.equal(calculatePreservedScrollTop({
    previousScrollTop: 400,
    previousScrollHeight: 1000,
    clientHeight: 380,
    nextScrollHeight: 700,
  }), 320);
});
