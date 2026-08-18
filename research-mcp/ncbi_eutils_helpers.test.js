import test from "node:test";
import assert from "node:assert/strict";

import {
  createSerializedRequestPacer,
  getNcbiEutilsMinIntervalMs,
  isNcbiEutilsUrl,
  summarizeNcbiEutilsError,
} from "./ncbi_eutils_helpers.js";

test("recognizes only NCBI E-utilities requests", () => {
  assert.equal(isNcbiEutilsUrl("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed"), true);
  assert.equal(isNcbiEutilsUrl("https://www.ncbi.nlm.nih.gov/snp/rs334"), false);
  assert.equal(isNcbiEutilsUrl("https://eutils.ncbi.nlm.nih.gov/other"), false);
});

test("uses NCBI request intervals for anonymous and API-key traffic", () => {
  assert.equal(getNcbiEutilsMinIntervalMs(""), 350);
  assert.equal(getNcbiEutilsMinIntervalMs("configured"), 110);
});

test("serializes concurrent requests with the configured minimum interval", async () => {
  let currentTime = 0;
  const delays = [];
  const waitForSlot = createSerializedRequestPacer({
    minIntervalMs: 350,
    now: () => currentTime,
    sleep: async (delayMs) => {
      delays.push(delayMs);
      currentTime += delayMs;
    },
  });

  await Promise.all([waitForSlot(), waitForSlot(), waitForSlot(), waitForSlot()]);

  assert.deepEqual(delays, [350, 350, 350]);
});

test("summarizes NCBI rate-limit errors without leaking URLs or upstream payloads", () => {
  const raw = "Request failed (429): https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?api_key=secret | {\"error\":\"API rate limit exceeded\"}";
  const summary = summarizeNcbiEutilsError(raw);

  assert.equal(summary, "NCBI E-utilities rate limit was reached after bounded retries.");
  assert.equal(summary.includes("https://"), false);
  assert.equal(summary.includes("secret"), false);
});
