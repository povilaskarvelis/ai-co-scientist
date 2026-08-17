import test from "node:test";
import assert from "node:assert/strict";

import {
  filterNEMARByModalities,
  normalizeNEMARDatasetRecord,
  normalizeNEMARSearchPayload,
} from "./archive_helpers.js";

test("normalizes the current NEMAR search response", () => {
  const payload = normalizeNEMARSearchPayload({
    count: 1,
    method: "semantic",
    results: [{
      id: "on004100",
      name: "HUP iEEG Epilepsy Dataset",
      modalities: "ieeg",
      participants: 58,
      doi: "10.82901/nemar.on004100",
      has_hed: 1,
    }],
  });

  assert.equal(payload.total, 1);
  assert.equal(payload.method, "semantic");
  assert.equal(payload.items[0].description_name, "HUP iEEG Epilepsy Dataset");
  assert.equal(payload.items[0].hasHED, true);
  assert.deepEqual(filterNEMARByModalities(payload.items, ["iEEG"]), payload.items);
  assert.deepEqual(filterNEMARByModalities(payload.items, ["EEG"]), []);
});

test("normalizes current NEMAR detail fields", () => {
  const record = normalizeNEMARDatasetRecord({
    dataset_id: "nm000110",
    name: "CHB-MIT",
    subject_count: 24,
    total_files: 686,
    sessions_count: 1,
    latest_version: "v1.0.1",
    bids_version: "1.9.0",
    hed_version: "8.3.0",
    updated_at: "2026-03-11 00:00:00",
  });

  assert.equal(record.id, "nm000110");
  assert.equal(record.participants, 24);
  assert.equal(record.totalFiles, 686);
  assert.equal(record.sessionsNum, 1);
  assert.equal(record.latestSnapshot, "v1.0.1");
  assert.equal(record.publishDate, "2026-03-11 00:00:00");
});
