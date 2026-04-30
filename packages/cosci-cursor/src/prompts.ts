export type Stage = "plan" | "evidence" | "report"

type PromptOptions = {
  question: string
  slug: string
}

const COMMON_INSTRUCTIONS = [
  "You are running AI Co-Scientist as a repo-native biomedical investigation.",
  "Follow AGENTS.md and the biomedical-investigation skill.",
  "The local artifacts are the source of truth; do not leave the only copy of useful state in chat.",
  "Write all artifacts under .co-scientist/investigations/<slug>/.",
  "Prefer project MCP tools from research-mcp when they are available and relevant.",
  "Preserve concrete source identifiers and structured numeric metadata whenever available.",
].join("\n")

export function buildStagePrompt(stage: Stage, options: PromptOptions) {
  switch (stage) {
    case "plan":
      return buildPlanPrompt(options)
    case "evidence":
      return buildEvidencePrompt(options)
    case "report":
      return buildReportPrompt(options)
  }
}

export function buildRepairPrompt(
  stage: Stage,
  options: PromptOptions & { validationOutput: string; attempt: number }
) {
  return [
    COMMON_INSTRUCTIONS,
    "",
    `Investigation slug: ${options.slug}`,
    `Original question: ${options.question}`,
    `Validation stage: ${stage}`,
    `Repair attempt: ${options.attempt}`,
    "",
    "The artifact validator failed. Read the existing local artifacts, then edit only the investigation artifacts needed to satisfy the validator and preserve scientific accuracy.",
    "Do not delete useful evidence to make validation easier. If a field is missing, fill it from the local evidence or perform a targeted lookup.",
    "",
    "Validator output:",
    fenced(options.validationOutput),
  ].join("\n")
}

function buildPlanPrompt(options: PromptOptions) {
  return [
    COMMON_INSTRUCTIONS,
    "",
    `Investigation slug: ${options.slug}`,
    `Original question: ${options.question}`,
    "",
    "Create or update these files:",
    `- .co-scientist/investigations/${options.slug}/question.md`,
    `- .co-scientist/investigations/${options.slug}/plan.json`,
    "",
    "For this stage, only plan the investigation. Do not write evidence, claims, or report artifacts yet.",
    "The plan must use schema co_scientist.plan.v1, machine-actionable tool_hint values, optional source_notes for human-readable nuance, and step statuses of pending.",
  ].join("\n")
}

function buildEvidencePrompt(options: PromptOptions) {
  return [
    COMMON_INSTRUCTIONS,
    "",
    `Investigation slug: ${options.slug}`,
    `Original question: ${options.question}`,
    "",
    `Read .co-scientist/investigations/${options.slug}/plan.json and execute the evidence-gathering steps.`,
    `Append source-grounded records to .co-scientist/investigations/${options.slug}/evidence.jsonl using schema co_scientist.evidence.v1.`,
    "Update plan step statuses as you complete, block, or skip steps. If evidence exists for a step, that step must not remain pending or in_progress.",
    "",
    "Evidence records should be rich enough for later synthesis: preserve PMIDs/DOIs/NCT IDs/database IDs, numeric values, assay context, clinical phase/status, tissue/cell-type context, and caveats in metadata.",
    "Do not write claims.jsonl or report.md yet unless they already exist and need a minimal consistency update.",
  ].join("\n")
}

function buildReportPrompt(options: PromptOptions) {
  return [
    COMMON_INSTRUCTIONS,
    "",
    `Investigation slug: ${options.slug}`,
    `Original question: ${options.question}`,
    "",
    `Read .co-scientist/investigations/${options.slug}/plan.json and evidence.jsonl.`,
    `Write .co-scientist/investigations/${options.slug}/claims.jsonl using schema co_scientist.claim.v1.`,
    `Write .co-scientist/investigations/${options.slug}/report.md as a user-facing cited Markdown report.`,
    `Write .co-scientist/investigations/${options.slug}/run_notes.md with concise provenance and follow-up notes.`,
    "",
    "Set plan.status to synthesized when report-stage artifacts are complete. Every plan step should be completed, blocked, or skipped.",
    "Keep internal process notes out of report.md. Put caveats, failed lookups, and edit history in run_notes.md.",
  ].join("\n")
}

function fenced(value: string) {
  return ["```text", value.trim(), "```"].join("\n")
}
