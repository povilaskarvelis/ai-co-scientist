import assert from "node:assert/strict";
import test from "node:test";

import {
  getHpoSearchQueryVariants,
  rankHpoSearchDocs,
} from "./hpo_helpers.js";

test("adds a singular fallback for a broad plural phenotype query", () => {
  assert.deepEqual(getHpoSearchQueryVariants("seizures"), ["seizures", "seizure"]);
  assert.deepEqual(getHpoSearchQueryVariants("developmental delay"), ["developmental delay"]);
});

test("ranks the generic exact phenotype ahead of an API-ordered subtype", () => {
  const ranked = rankHpoSearchDocs([
    {
      docs: [{
        label: "Bilateral tonic-clonic seizure with focal onset",
        obo_id: "HP:0007334",
        ontology_name: "hp",
      }],
    },
    {
      docs: [{
        label: "Seizure",
        obo_id: "HP:0001250",
        ontology_name: "hp",
      }],
    },
  ], "seizures");

  assert.equal(ranked[0].obo_id, "HP:0001250");
});

test("deduplicates the same HPO term returned by primary and fallback searches", () => {
  const doc = { label: "Seizure", obo_id: "HP:0001250", ontology_name: "hp" };
  assert.equal(rankHpoSearchDocs([{ docs: [doc] }, { docs: [doc] }], "seizures").length, 1);
});
