const test = require("node:test");
const assert = require("node:assert/strict");

const {
  calculatePreservedScrollTop,
  cleanStepFinding,
  computePlanProgress,
  formatElapsed,
  planStatusText,
  planningStage,
  planningStageLabel,
  recordUrl,
  sanitizeDisplaySummary,
  shouldUseStartingPlaceholder,
  stepDetailView,
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

const PLAN_STEPS = [{ id: "S1" }, { id: "S2" }, { id: "S3" }];

test("plan checklist shows no progress while the plan awaits approval", () => {
  const progress = computePlanProgress({ taskStatus: "in_progress", awaitingApproval: true, steps: PLAN_STEPS });

  assert.equal(progress.phase, "awaiting");
  assert.deepEqual(Object.values(progress.stepStates).map((step) => step.status), ["pending", "pending", "pending"]);
  assert.equal(planStatusText(progress), "");
});

test("plan checklist marks finished, running and queued steps from the live run", () => {
  const progress = computePlanProgress({
    runStatus: "running",
    started: true,
    events: [
      { type: "step.started", at: "2026-10-06T10:00:00Z", metrics: { step_id: "S1" } },
      { type: "tool.called", human_line: "Fetching DailyMed label", metrics: { tool: "get_dailymed_drug_label" } },
      { type: "step.started", at: "2026-10-06T10:00:20Z", metrics: { step_id: "S2" } },
      { type: "tool.called", human_line: "Searching clinical trials for donanemab", metrics: { step_id: "S2" } },
    ],
    summaries: [{
      step_details: [
        { id: "S1", status: "completed", step_progress_note: "Findings Summary The Leqembi label carries a boxed warning for ARIA, the brain swelling seen early in treatment." },
        { id: "S2", status: "in_progress" },
        { id: "S3", status: "pending" },
      ],
    }],
    steps: PLAN_STEPS,
  });

  assert.equal(progress.phase, "running");
  assert.equal(progress.stepStates.S1.status, "completed");
  assert.equal(progress.stepStates.S1.line, "The Leqembi label carries a boxed warning for ARIA, the brain swelling seen early in treatment.");
  assert.deepEqual(progress.stepStates.S2, { status: "in_progress", line: "Searching clinical trials for donanemab" });
  assert.equal(progress.stepStates.S3.status, "pending");
  assert.equal(
    planStatusText(progress, Date.parse("2026-10-06T10:01:23Z")),
    "Researching \u00b7 1:23 \u00b7 usually 2\u20135 min",
  );
});

test("plan checklist shows the first step as starting before any tool call arrives", () => {
  const progress = computePlanProgress({ runStatus: "queued", started: true, steps: PLAN_STEPS });

  assert.deepEqual(progress.stepStates.S1, { status: "in_progress", line: "Starting\u2026" });
  assert.equal(progress.stepStates.S2.status, "pending");
});

test("plan checklist switches to report writing once every step has finished", () => {
  const progress = computePlanProgress({
    runStatus: "running",
    started: true,
    summaries: [{ step_details: PLAN_STEPS.map((step) => ({ id: step.id, status: step.id === "S3" ? "blocked" : "completed" })) }],
    steps: PLAN_STEPS,
  });

  assert.equal(progress.phase, "writing");
  assert.equal(progress.stepStates.S3.status, "blocked");
});

test("finished plan reports total research time from the saved log", () => {
  const progress = computePlanProgress({
    taskStatus: "completed",
    started: true,
    events: [
      { type: "execution.running", at: "2026-10-06T10:00:00Z" },
      { type: "run.completed", at: "2026-10-06T10:02:28Z" },
    ],
    steps: [{ id: "S1", status: "completed", result_summary: "Used PubMed." }],
  });

  assert.equal(progress.phase, "done");
  assert.equal(planStatusText(progress), "Done in 2:28");
});

test("step findings are cleaned to one readable sentence", () => {
  assert.equal(cleanStepFinding("**Key Findings** Short."), "Short.");
  assert.equal(
    cleanStepFinding("Summary: Pivotal trials were identified on ClinicalTrials.gov for both drugs. Further detail follows here."),
    "Pivotal trials were identified on ClinicalTrials.gov for both drugs.",
  );
  assert.ok(cleanStepFinding("x".repeat(400)).length <= 170);
  assert.equal(cleanStepFinding('{ "result": [ { "entity_type": "trial", "id": "NCT07617155" } ] }'), "");
});

test("planning labels and elapsed time read naturally", () => {
  assert.equal(planningStageLabel(1000), "Reading your question");
  assert.equal(planningStageLabel(6000), "Choosing which databases to search");
  assert.equal(planningStageLabel(25000), "Drafting a research plan");
  assert.equal(formatElapsed(65000), "1:05");
});

test("an expanded step shows its finding, searches, linked records and open questions", () => {
  const view = stepDetailView({
    result_summary: "Used ClinicalTrials.gov. Two Phase 3 studies anchor the evidence: **NCT04468659** and NCT03887455.",
    evidence_ids: ["NCT04468659", "PMID:38253184", "CHEMBL25", "NCT04468659"],
    open_gaps: ["Pivotal results are not posted yet"],
    tool_log: [
      {
        tool: "ClinicalTrials.gov",
        raw_tool: "search_clinical_trials",
        summary: "Searching clinical trials for lecanemab",
        result: "Fetched 33 ClinicalTrials.gov study records; more may exist.",
      },
      { tool: "load_skill", raw_tool: "load_skill", summary: "Loading skill", result: "ok" },
    ],
  });

  assert.equal(view.finding, "Two Phase 3 studies anchor the evidence: **NCT04468659** and NCT03887455.");
  assert.deepEqual(view.sources, ["ClinicalTrials.gov"]);
  assert.deepEqual(view.searches, [
    { source: "ClinicalTrials.gov", query: "Searching clinical trials for lecanemab", result: "Fetched 33 ClinicalTrials.gov study records" },
  ]);
  assert.deepEqual(view.records.map((record) => record.id), ["NCT04468659", "PMID:38253184", "CHEMBL25"]);
  assert.equal(view.records[2].url, "");
  assert.deepEqual(view.gaps, ["Pivotal results are not posted yet"]);
});

test("record identifiers link to their source pages", () => {
  assert.equal(recordUrl("PMID:38253184"), "https://pubmed.ncbi.nlm.nih.gov/38253184/");
  assert.equal(recordUrl("NCT04468659"), "https://clinicaltrials.gov/study/NCT04468659");
  assert.equal(recordUrl("DOI:10.1016/j.cell.2023.09.023"), "https://doi.org/10.1016/j.cell.2023.09.023");
  assert.equal(recordUrl("PMC11167451"), "https://pmc.ncbi.nlm.nih.gov/articles/PMC11167451/");
  assert.equal(recordUrl("rs429358"), "https://www.ncbi.nlm.nih.gov/snp/rs429358");
  assert.equal(recordUrl("E12"), "");
});

test("a running step lists its live searches until its step log arrives", () => {
  const progress = computePlanProgress({
    runStatus: "running",
    started: true,
    events: [
      { type: "step.started", metrics: { step_id: "S1" } },
      { type: "tool.called", human_line: "Fetching DailyMed label for Leqembi", metrics: { step_id: "S1" } },
      { type: "tool.called", human_line: "Fetching DailyMed label for Kisunla", metrics: { step_id: "S1" } },
    ],
    steps: PLAN_STEPS,
  });

  assert.deepEqual(progress.stepViews.S1.searches.map((search) => search.query), [
    "Fetching DailyMed label for Leqembi",
    "Fetching DailyMed label for Kisunla",
  ]);
  assert.equal(progress.stepViews.S2, undefined);
});

test("each planning stage names the orbiter it shows", () => {
  assert.deepEqual([1000, 6000, 25000].map((ms) => planningStage(ms).key), ["reading", "sources", "drafting"]);
});

test("the report row stays pending while steps run, writes after the last step and completes with the run", () => {
  const finished = { step_details: PLAN_STEPS.map((step) => ({ id: step.id, status: "completed" })) };
  const running = computePlanProgress({ runStatus: "running", started: true, steps: PLAN_STEPS });
  const writing = computePlanProgress({ runStatus: "running", started: true, summaries: [finished], steps: PLAN_STEPS });
  const done = computePlanProgress({ taskStatus: "completed", started: true, summaries: [finished], steps: PLAN_STEPS });
  const failed = computePlanProgress({ runStatus: "failed", started: true, summaries: [finished], steps: PLAN_STEPS });

  assert.equal(running.reportState.status, "pending");
  assert.deepEqual(writing.reportState, { status: "in_progress", line: "Writing a cited report from the findings\u2026" });
  assert.equal(done.reportState.status, "completed");
  assert.equal(failed.reportState.status, "blocked");
});
