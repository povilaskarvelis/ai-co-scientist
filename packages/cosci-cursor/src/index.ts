#!/usr/bin/env node

import { appendFileSync } from "node:fs"
import { mkdir, writeFile } from "node:fs/promises"
import path from "node:path"
import process from "node:process"
import { CursorAgentError } from "@cursor/sdk"
import { parseMcpMode, resolveMcpOptions, type McpMode } from "./mcp.js"
import { ensureQuestionFile, investigationDir, slugify } from "./paths.js"
import { buildRepairPrompt, buildStagePrompt, type Stage } from "./prompts.js"
import {
  CursorAgentSession,
  CursorRunStatusError,
  formatSdkError,
  type AgentEvent,
} from "./session.js"
import { validateInvestigation, validationOutput, type ValidationResult } from "./validate.js"

const STAGES: Stage[] = ["plan", "evidence", "report"]

type RunOptions = {
  command: "run"
  question: string
  slug: string
  cwd: string
  model: string
  maxRepairs: number
  force: boolean
  dryRun: boolean
  agentId?: string
  mcpMode: McpMode
}

type ValidateOptions = {
  command: "validate"
  slug: string
  cwd: string
  stage: Stage
}

type HelpOptions = {
  command: "help"
}

type CliOptions = RunOptions | ValidateOptions | HelpOptions

type PhaseResult = {
  stage: Stage
  attempts: number
  validation: ValidationResult
}

type RunManifest = {
  schema: "co_scientist.cursor_run.v1"
  slug: string
  question: string
  cwd: string
  model: string
  dry_run: boolean
  agent_id?: string
  resumed_agent: boolean
  mcp: {
    mode: McpMode
    summary: string
  }
  started_at: string
  completed_at?: string
  status: "running" | "passed" | "failed"
  agent_runs: AgentRunSummary[]
  phases: PhaseResult[]
}

type AgentRunSummary = {
  stage: Stage
  repair: boolean
  agent_id: string
  run_id: string
  status?: string
  duration_ms?: number
}

class PipelineValidationError extends Error {
  readonly stage: Stage
  readonly attempts: number

  constructor(stage: Stage, attempts: number) {
    super(`${stage} validation failed after ${attempts} attempt(s).`)
    this.name = "PipelineValidationError"
    this.stage = stage
    this.attempts = attempts
  }
}

async function main() {
  const options = await parseArgs(process.argv.slice(2))

  if (options.command === "help") {
    printHelp()
    return
  }

  if (options.command === "validate") {
    const result = await validateInvestigation(options.cwd, options.slug, options.stage)
    process.stdout.write(result.stdout)
    process.stderr.write(result.stderr)
    process.exitCode = result.ok ? 0 : 1
    return
  }

  await runInvestigation(options)
}

async function runInvestigation(options: RunOptions) {
  const artifactRoot = await ensureQuestionFile(options.cwd, options.slug, options.question)
  const mcp = options.dryRun
    ? {
        mode: options.mcpMode,
        summary: `${options.mcpMode} MCP selected; not resolved during dry-run`,
      }
    : await resolveMcpOptions(options.cwd, options.mcpMode)
  const manifest: RunManifest = {
    schema: "co_scientist.cursor_run.v1",
    slug: options.slug,
    question: options.question,
    cwd: options.cwd,
    model: options.model,
    dry_run: options.dryRun,
    agent_id: options.agentId,
    resumed_agent: Boolean(options.agentId),
    mcp: {
      mode: mcp.mode,
      summary: mcp.summary,
    },
    started_at: new Date().toISOString(),
    status: "running",
    agent_runs: [],
    phases: [],
  }
  await saveManifest(artifactRoot, manifest)

  if (options.dryRun) {
    for (const stage of STAGES) {
      console.log(`\n# ${stage} prompt\n`)
      console.log(buildStagePrompt(stage, options))
    }
    manifest.status = "passed"
    manifest.completed_at = new Date().toISOString()
    await saveManifest(artifactRoot, manifest)
    return
  }

  const apiKey = process.env.CURSOR_API_KEY
  if (!apiKey) {
    throw new Error("CURSOR_API_KEY is required to run the Cursor SDK agent.")
  }

  let session: CursorAgentSession | undefined

  try {
    session = await CursorAgentSession.create({
      apiKey,
      cwd: options.cwd,
      model: { id: options.model },
      force: options.force,
      agentId: options.agentId,
      mcpServers: mcp.mcpServers,
      settingSources: mcp.settingSources,
    })
    manifest.agent_id = session.agentId
    manifest.resumed_agent = session.wasResumed
    await saveManifest(artifactRoot, manifest)
    console.log(`Cursor agent: ${session.agentId}${session.wasResumed ? " (resumed)" : ""}`)
    console.log(`MCP: ${mcp.summary}`)

    for (const stage of STAGES) {
      const result = await runStage(session, options, artifactRoot, stage, manifest)
      manifest.phases.push(result)
      await saveManifest(artifactRoot, manifest)

      if (!result.validation.ok) {
        manifest.status = "failed"
        manifest.completed_at = new Date().toISOString()
        await saveManifest(artifactRoot, manifest)
        throw new PipelineValidationError(stage, result.attempts)
      }
    }

    manifest.status = "passed"
    manifest.completed_at = new Date().toISOString()
    await saveManifest(artifactRoot, manifest)
  } catch (error) {
    manifest.status = "failed"
    manifest.completed_at = new Date().toISOString()
    await saveManifest(artifactRoot, manifest)
    throw error
  } finally {
    await session?.dispose()
  }
}

async function runStage(
  session: CursorAgentSession,
  options: RunOptions,
  artifactRoot: string,
  stage: Stage,
  manifest: RunManifest
): Promise<PhaseResult> {
  console.log(`\n== ${stage}: agent pass ==`)
  await sendAndLog(session, artifactRoot, stage, buildStagePrompt(stage, options), manifest, false)

  let validation = await validateAndPrint(options.cwd, options.slug, stage)
  let attempts = 1

  while (!validation.ok && attempts <= options.maxRepairs) {
    console.log(`\n== ${stage}: repair ${attempts} ==`)
    await sendAndLog(
      session,
      artifactRoot,
      stage,
      buildRepairPrompt(stage, {
        ...options,
        attempt: attempts,
        validationOutput: validationOutput(validation),
      }),
      manifest,
      true
    )
    attempts += 1
    validation = await validateAndPrint(options.cwd, options.slug, stage)
  }

  return { stage, attempts, validation }
}

async function sendAndLog(
  session: CursorAgentSession,
  artifactRoot: string,
  stage: Stage,
  prompt: string,
  manifest: RunManifest,
  repair: boolean
) {
  let runSummary: AgentRunSummary | undefined
  try {
    await session.sendPrompt({
      prompt,
      onEvent: asyncEventLogger(artifactRoot, stage, (event) => {
        if (event.type === "run_started") {
          runSummary = {
            stage,
            repair,
            agent_id: event.agentId,
            run_id: event.runId,
          }
          manifest.agent_runs.push(runSummary)
        } else if (event.type === "result" && runSummary) {
          runSummary.status = event.status
          runSummary.duration_ms = event.durationMs
        }
      }),
    })
  } finally {
    await saveManifest(artifactRoot, manifest)
  }
}

function asyncEventLogger(
  artifactRoot: string,
  stage: Stage,
  onEvent?: (event: AgentEvent) => void
) {
  return (event: AgentEvent) => {
    onEvent?.(event)
    renderEvent(event)
    const line = JSON.stringify({
      ts: new Date().toISOString(),
      stage,
      event,
    })
    appendFileSync(path.join(artifactRoot, "cursor-run.jsonl"), `${line}\n`, "utf8")
  }
}

function renderEvent(event: AgentEvent) {
  switch (event.type) {
    case "agent_ready":
      console.log(`\n[agent] ${event.agentId}${event.resumed ? " resumed" : ""}`)
      break
    case "run_started":
      console.log(`\n[run] ${event.runId} agent=${event.agentId}`)
      break
    case "assistant_delta":
      process.stdout.write(event.text)
      break
    case "thinking":
      break
    case "tool":
      console.log(`\n[tool:${event.status}] ${event.name}${event.params ? ` ${event.params}` : ""}`)
      break
    case "status":
      console.log(`\n[status:${event.status}]${event.message ? ` ${event.message}` : ""}`)
      break
    case "task":
      if (event.text) {
        console.log(`\n[task${event.status ? `:${event.status}` : ""}] ${event.text}`)
      }
      break
    case "system":
      console.log(
        `\n[system]${event.model ? ` model=${formatModel(event.model)}` : ""}${
          event.tools?.length ? ` tools=${event.tools.length}` : ""
        }`
      )
      break
    case "request":
      console.log(`\n[request] ${event.requestId}`)
      break
    case "user":
      break
    case "stream_warning":
      console.warn(`\n[stream-warning] ${event.message}`)
      break
    case "result":
      console.log(
        `\n[result:${event.status}]${event.runId ? ` ${event.runId}` : ""}${
          event.durationMs ? ` ${formatDuration(event.durationMs)}` : ""
        }`
      )
      break
  }
}

async function validateAndPrint(cwd: string, slug: string, stage: Stage) {
  console.log(`\n== ${stage}: validation ==`)
  const result = await validateInvestigation(cwd, slug, stage)
  process.stdout.write(result.stdout)
  process.stderr.write(result.stderr)
  return result
}

async function saveManifest(artifactRoot: string, manifest: RunManifest) {
  await mkdir(artifactRoot, { recursive: true })
  await writeFile(
    path.join(artifactRoot, "cursor-run-manifest.json"),
    `${JSON.stringify(manifest, null, 2)}\n`,
    "utf8"
  )
}

async function parseArgs(argv: string[]): Promise<CliOptions> {
  const [command = "help", ...rest] = argv
  if (command === "help" || command === "--help" || command === "-h") {
    return { command: "help" }
  }
  if (command === "validate") {
    return parseValidateArgs(rest)
  }
  if (command === "run") {
    return parseRunArgs(rest)
  }
  throw new Error(`Unknown command: ${command}`)
}

function parseValidateArgs(argv: string[]): ValidateOptions {
  const values = [...argv]
  const cwd = path.resolve(takeOption(values, "--cwd") ?? process.cwd())
  const stage = parseStage(takeOption(values, "--stage") ?? "report")
  const slug = values.shift()
  if (!slug) {
    throw new Error("validate requires a slug")
  }
  if (values.length) {
    throw new Error(`Unexpected validate argument(s): ${values.join(" ")}`)
  }
  return { command: "validate", slug, cwd, stage }
}

async function parseRunArgs(argv: string[]): Promise<RunOptions> {
  const values = [...argv]
  const cwd = path.resolve(takeOption(values, "--cwd") ?? process.cwd())
  const slugOption = takeOption(values, "--slug")
  const model = takeOption(values, "--model") ?? process.env.CURSOR_MODEL ?? "composer-2"
  const maxRepairs = Number(takeOption(values, "--max-repairs") ?? "2")
  const agentId = takeOption(values, "--agent-id")
  const mcpMode = parseMcpMode(takeOption(values, "--mcp") ?? "inline")
  const force = takeFlag(values, "--force")
  const dryRun = takeFlag(values, "--dry-run")
  const explicitQuestion = takeOption(values, "--question")
  if (explicitQuestion && values.length) {
    throw new Error(`Unexpected run argument(s) with --question: ${values.join(" ")}`)
  }
  const positionalQuestion = values.join(" ")
  const question = (explicitQuestion ?? (positionalQuestion || (await readStdin()))).trim()

  if (!question) {
    throw new Error("run requires a question argument, --question, or stdin.")
  }
  if (!Number.isInteger(maxRepairs) || maxRepairs < 0) {
    throw new Error("--max-repairs must be a non-negative integer")
  }

  const slug = slugOption ?? slugify(question)
  return { command: "run", question, slug, cwd, model, maxRepairs, force, dryRun, agentId, mcpMode }
}

function takeOption(values: string[], name: string) {
  const index = values.indexOf(name)
  if (index === -1) {
    return undefined
  }
  const value = values[index + 1]
  if (!value) {
    throw new Error(`${name} requires a value`)
  }
  values.splice(index, 2)
  return value
}

function takeFlag(values: string[], name: string) {
  const index = values.indexOf(name)
  if (index === -1) {
    return false
  }
  values.splice(index, 1)
  return true
}

function parseStage(value: string): Stage {
  if (value === "plan" || value === "evidence" || value === "report") {
    return value
  }
  throw new Error(`Invalid stage: ${value}`)
}

async function readStdin() {
  if (process.stdin.isTTY) {
    return ""
  }
  return new Promise<string>((resolve, reject) => {
    let text = ""
    process.stdin.setEncoding("utf8")
    process.stdin.on("data", (chunk) => {
      text += chunk
    })
    process.stdin.on("end", () => resolve(text))
    process.stdin.on("error", reject)
  })
}

function formatDuration(ms: number) {
  if (ms < 1000) {
    return `${ms}ms`
  }
  return `${(ms / 1000).toFixed(1)}s`
}

function formatModel(model: { id: string; params?: Array<{ id: string; value: string }> }) {
  const params = model.params?.map((param) => `${param.id}=${param.value}`).join(",")
  return params ? `${model.id}(${params})` : model.id
}

function printHelp() {
  console.log(`AI Co-Scientist Cursor SDK runner

Usage:
  cosci-cursor run "Evaluate LRRK2 as a Parkinson's therapeutic target"
  cosci-cursor run --slug lrrk2-parkinsons --model composer-2 --max-repairs 2 "Question"
  cosci-cursor validate lrrk2-parkinsons --stage report

Options:
  --cwd <path>          Repository root. Defaults to current directory.
  --slug <slug>        Investigation slug. Defaults to a slug from the question.
  --model <id>         Cursor model id. Defaults to CURSOR_MODEL or composer-2.
  --agent-id <id>      Resume an existing local Cursor agent instead of creating one.
  --mcp <mode>         MCP mode: inline, project, or off. Defaults to inline.
  --max-repairs <n>    Validator repair attempts per stage. Defaults to 2.
  --force              Pass local.force=true to Cursor SDK.
  --dry-run            Write question.md and print prompts without calling Cursor.

Environment:
  CURSOR_API_KEY       Required for run unless --dry-run is used.
  PYTHON               Optional Python executable for artifact validation.
`)
}

main().catch((error: unknown) => {
  if (error instanceof CursorRunStatusError) {
    console.error(
      `ERROR: Cursor run failed after starting: run=${error.result.id} status=${error.result.status}`
    )
    if (error.result.durationMs) {
      console.error(`Duration: ${formatDuration(error.result.durationMs)}`)
    }
    process.exitCode = 2
    return
  }

  if (error instanceof CursorAgentError) {
    console.error(`ERROR: Cursor SDK startup/config error: ${formatSdkError(error)}`)
    process.exitCode = 1
    return
  }

  if (error instanceof PipelineValidationError) {
    console.error(`ERROR: ${error.message}`)
    process.exitCode = 3
    return
  }

  const message = error instanceof Error ? `${error.name}: ${error.message}` : String(error)
  console.error(`ERROR: ${message}`)
  process.exitCode = 1
})
