import {
  Agent,
  CursorAgentError,
  type McpServerConfig,
  type ModelSelection,
  type Run,
  type RunResult,
  type SDKAgent,
  type SDKMessage,
  type SettingSource,
} from "@cursor/sdk"

export type TokenUsage = {
  inputTokens?: number
  outputTokens?: number
}

export type AgentEvent =
  | { type: "agent_ready"; agentId: string; resumed: boolean }
  | { type: "run_started"; agentId: string; runId: string }
  | { type: "assistant_delta"; text: string }
  | { type: "thinking"; text: string }
  | { type: "tool"; callId?: string; name: string; params?: string; status: string }
  | { type: "status"; status: string; message?: string }
  | { type: "task"; status?: string; text?: string }
  | { type: "system"; model?: ModelSelection; tools?: string[] }
  | { type: "request"; requestId: string }
  | { type: "user" }
  | { type: "stream_warning"; message: string }
  | { type: "result"; status: string; runId?: string; durationMs?: number; usage?: TokenUsage }

type CursorAgentSessionOptions = {
  apiKey: string
  cwd: string
  model: ModelSelection
  force?: boolean
  agentId?: string
  mcpServers?: Record<string, McpServerConfig>
  name?: string
  settingSources?: SettingSource[]
}

type SendPromptOptions = {
  prompt: string
  onEvent: (event: AgentEvent) => void
}

export class CursorRunStatusError extends Error {
  readonly result: RunResult

  constructor(result: RunResult) {
    super(`Cursor run ${result.id} finished with status ${result.status}`)
    this.name = "CursorRunStatusError"
    this.result = result
  }
}

export class CursorAgentSession {
  private currentRun: Run | null = null
  private readonly agent: SDKAgent
  private readonly force: boolean
  private readonly model: ModelSelection
  private readonly resumed: boolean

  private constructor(agent: SDKAgent, options: CursorAgentSessionOptions) {
    this.agent = agent
    this.force = Boolean(options.force)
    this.model = options.model
    this.resumed = Boolean(options.agentId)
  }

  static async create(options: CursorAgentSessionOptions) {
    const agentOptions = {
      apiKey: options.apiKey,
      name: options.name ?? "AI Co-Scientist local runner",
      model: options.model,
      local: {
        cwd: options.cwd,
        ...(options.settingSources?.length ? { settingSources: options.settingSources } : {}),
      },
      ...(options.mcpServers ? { mcpServers: options.mcpServers } : {}),
    }

    const agent = options.agentId
      ? await Agent.resume(options.agentId, agentOptions)
      : await Agent.create(agentOptions)

    return new CursorAgentSession(agent, options)
  }

  get agentId() {
    return this.agent.agentId
  }

  get wasResumed() {
    return this.resumed
  }

  async sendPrompt({ prompt, onEvent }: SendPromptOptions) {
    const run = await this.agent.send(prompt, {
      model: this.model,
      ...(this.force ? { local: { force: true } } : {}),
    })
    this.currentRun = run
    onEvent({ type: "run_started", agentId: this.agent.agentId, runId: run.id })

    try {
      let streamError: unknown

      if (run.supports("stream")) {
        try {
          for await (const event of run.stream()) {
            emitSdkMessage(event, onEvent)
          }
        } catch (error) {
          streamError = error
          onEvent({
            type: "stream_warning",
            message: formatSdkError(error),
          })
        }
      } else {
        onEvent({
          type: "stream_warning",
          message: run.unsupportedReason("stream") ?? "run.stream is not supported by this runtime",
        })
      }

      if (!run.supports("wait")) {
        throw new Error(run.unsupportedReason("wait") ?? "run.wait is not supported by this runtime")
      }

      const result = await run.wait()
      onEvent({
        type: "result",
        status: result.status,
        runId: result.id,
        durationMs: result.durationMs,
        usage: (result as { usage?: TokenUsage }).usage,
      })

      if (result.status !== "finished") {
        throw new CursorRunStatusError(result)
      }

      if (streamError instanceof CursorAgentError) {
        onEvent({
          type: "stream_warning",
          message: `stream had SDK error after terminal wait succeeded: ${formatSdkError(streamError)}`,
        })
      }

      return result
    } finally {
      if (this.currentRun === run) {
        this.currentRun = null
      }
    }
  }

  async dispose() {
    const disposable = this.agent as unknown as {
      [Symbol.asyncDispose]?: () => Promise<void>
    }
    await disposable[Symbol.asyncDispose]?.()
  }
}

function emitSdkMessage(event: SDKMessage, emit: (event: AgentEvent) => void) {
  switch (event.type) {
    case "assistant":
      for (const block of event.message.content) {
        if (block.type === "text") {
          emit({ type: "assistant_delta", text: block.text })
        } else {
          emit({
            type: "tool",
            callId: block.id,
            name: block.name,
            params: summarizeToolArgs(block.name, block.input),
            status: "requested",
          })
        }
      }
      break
    case "thinking":
      emit({ type: "thinking", text: event.text })
      break
    case "tool_call":
      emit({
        type: "tool",
        callId: event.call_id,
        name: event.name,
        params: summarizeToolArgs(event.name, event.args),
        status: event.status,
      })
      break
    case "status":
      emit({ type: "status", status: event.status, message: event.message })
      break
    case "task":
      emit({ type: "task", status: event.status, text: event.text })
      break
    case "system":
      emit({ type: "system", model: event.model, tools: event.tools })
      break
    case "request":
      emit({ type: "request", requestId: event.request_id })
      break
    case "user":
      emit({ type: "user" })
      break
    default:
      break
  }
}

export function formatSdkError(error: unknown) {
  if (error instanceof CursorAgentError) {
    const details = [
      error.constructor.name,
      error.message,
      `retryable=${error.isRetryable}`,
      error.code ? `code=${error.code}` : undefined,
      error.status ? `status=${error.status}` : undefined,
      error.requestId ? `requestId=${error.requestId}` : undefined,
    ].filter(Boolean)
    return details.join(" ")
  }

  return error instanceof Error ? `${error.name}: ${error.message}` : String(error)
}

function summarizeToolArgs(toolName: string, args: unknown) {
  if (!args || typeof args !== "object") {
    return undefined
  }

  const record = args as Record<string, unknown>
  const keyGroups = getToolSummaryKeys(toolName)
  const parts: string[] = []

  for (const keys of keyGroups) {
    const part = summarizeFirstValue(record, keys)
    if (part) {
      parts.push(part)
    }
  }

  return parts.length > 0 ? parts.join(" ") : undefined
}

function getToolSummaryKeys(toolName: string) {
  const name = toolName.toLowerCase()

  if (name.includes("read")) {
    return [["path", "filePath", "target_file", "absolutePath"], ["offset"], ["limit"]]
  }
  if (name.includes("glob")) {
    return [["pattern", "glob", "glob_pattern"], ["path", "cwd", "target_directory"]]
  }
  if (name.includes("grep") || name.includes("search")) {
    return [["pattern", "query"], ["path"], ["glob"], ["type"]]
  }
  if (name.includes("shell") || name.includes("terminal") || name.includes("command")) {
    return [["command", "cmd"], ["cwd", "working_directory"]]
  }
  if (name.includes("edit") || name.includes("write") || name.includes("patch")) {
    return [["path", "target_file", "file"], ["instruction"]]
  }

  return [
    ["path", "file", "target_file"],
    ["pattern", "query", "command"],
  ]
}

function summarizeFirstValue(record: Record<string, unknown>, keys: string[]) {
  for (const key of keys) {
    const value = record[key]
    const formatted = formatArgValue(value)

    if (formatted) {
      return `${key}=${formatted}`
    }
  }

  return undefined
}

function formatArgValue(value: unknown): string | undefined {
  if (typeof value === "string") {
    return shortenValue(value.replace(/\s+/g, " ").trim())
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value)
  }
  if (Array.isArray(value)) {
    const items = value
      .slice(0, 3)
      .map(formatArgValue)
      .filter((item): item is string => Boolean(item))
    return items.length > 0 ? `[${items.join(",")}]` : undefined
  }

  return undefined
}

function shortenValue(value: string, maxLength = 80) {
  if (value.length <= maxLength) {
    return value
  }

  return `${value.slice(0, maxLength - 3)}...`
}
