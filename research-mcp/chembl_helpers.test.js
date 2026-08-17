import assert from "node:assert/strict";
import test from "node:test";

import {
  filterChEMBLTargetEntries,
  formatChEMBLActivitySummary,
  selectHumanChEMBLTargetIds,
} from "./chembl_helpers.js";

test("SHP2 resolves to the human single-protein ChEMBL target", () => {
  const targetIds = selectHumanChEMBLTargetIds([
    { target_chembl_id: "CHEMBL2620", organism: "Mus musculus", target_type: "SINGLE PROTEIN" },
    { target_chembl_id: "CHEMBL3864", organism: "Homo sapiens", target_type: "SINGLE PROTEIN" },
    { target_chembl_id: "CHEMBL4879528", organism: "Homo sapiens", target_type: "PROTEIN-PROTEIN INTERACTION" },
  ]);

  assert.deepEqual(targetIds, ["CHEMBL3864"]);
});

test("target aliases can match entries by resolved ChEMBL target ID", () => {
  const entries = [{
    target: "Tyrosine-protein phosphatase non-receptor type 11",
    targetChemblId: "CHEMBL3864",
    type: "IC50",
    units: "nM",
    values: [71],
  }];

  assert.deepEqual(filterChEMBLTargetEntries(entries, "SHP2", ["CHEMBL3864"]), entries);
});

test("empty target-filter results never render undefined potency", () => {
  const summary = formatChEMBLActivitySummary({
    activityCount: 28,
    chemblId: "CHEMBL4650521",
    entries: [],
    targetFilter: "SHP2",
  });

  assert.match(summary, /none matching target filter "SHP2"/);
  assert.doesNotMatch(summary, /undefined|0\.0 nM/);
});
