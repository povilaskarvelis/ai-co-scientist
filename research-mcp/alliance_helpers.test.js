import assert from "node:assert/strict";
import test from "node:test";

import { normalizeAllianceGeneSummaryPayload } from "./alliance_helpers.js";

test("normalizes current nested Alliance gene-summary payloads", () => {
  const normalized = normalizeAllianceGeneSummaryPayload({
    category: "gene_summary",
    gene: {
      primaryExternalId: "HGNC:6990",
      dataProvider: { abbreviation: "RGD" },
      taxon: { curie: "NCBITaxon:9606", name: "Homo sapiens" },
      geneSymbol: { displayText: "MECP2" },
      geneFullName: { displayText: "methyl-CpG binding protein 2" },
      geneSynonyms: [{ displayText: "RTT" }],
      geneSecondaryIds: [{ secondaryId: "RGD:1349232" }],
      relatedNotes: [{ freeText: "A methyl-CpG binding protein." }],
    },
  });

  assert.deepEqual(normalized, {
    id: "HGNC:6990",
    symbol: "MECP2",
    name: "methyl-CpG binding protein 2",
    species: {
      name: "Homo sapiens",
      curie: "NCBITaxon:9606",
      dataProviderShortName: "RGD",
    },
    geneSynopsis: "A methyl-CpG binding protein.",
    synonyms: ["RTT"],
    secondaryIds: ["RGD:1349232"],
    dataProvider: "RGD",
    modCrossRefCompleteUrl: "",
  });
});

test("keeps compatibility with the former flat Alliance gene shape", () => {
  const normalized = normalizeAllianceGeneSummaryPayload({
    id: "HGNC:11998",
    symbol: "TP53",
    name: "tumor protein p53",
    species: { name: "Homo sapiens" },
    synonyms: ["P53"],
    secondaryIds: ["RGD:70502"],
  });

  assert.equal(normalized.id, "HGNC:11998");
  assert.equal(normalized.symbol, "TP53");
  assert.deepEqual(normalized.synonyms, ["P53"]);
});

