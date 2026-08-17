import test from "node:test";
import assert from "node:assert/strict";

import {
  selectOpenTargetsDiseaseAssociation,
  shouldUseLiveOpenTargetsApi,
} from "./open_targets_helpers.js";

test("routes only current Open Targets requests to the live API", () => {
  assert.equal(shouldUseLiveOpenTargetsApi(), true);
  assert.equal(shouldUseLiveOpenTargetsApi("latest"), true);
  assert.equal(shouldUseLiveOpenTargetsApi("current"), true);
  assert.equal(shouldUseLiveOpenTargetsApi("25.09"), false);
  assert.equal(shouldUseLiveOpenTargetsApi("September 2025"), false);
});

test("selects the first disease search candidate that has an association row", () => {
  const candidates = [
    { id: "HP_0001513", name: "Obesity" },
    { id: "MONDO_0011122", name: "obesity disorder" },
  ];
  const rows = [
    { disease: { id: "MONDO_0011122" }, score: 0.79 },
    { disease: { id: "HP_0001513" }, score: 0.74 },
  ];

  const selected = selectOpenTargetsDiseaseAssociation(candidates, rows);

  assert.equal(selected.candidate.id, "HP_0001513");
  assert.equal(selected.row.score, 0.74);
});
