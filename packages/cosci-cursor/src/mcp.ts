import { access } from "node:fs/promises"
import path from "node:path"
import type { McpServerConfig, SettingSource } from "@cursor/sdk"

export type McpMode = "inline" | "project" | "off"

export type ResolvedMcpOptions = {
  mode: McpMode
  mcpServers?: Record<string, McpServerConfig>
  settingSources?: SettingSource[]
  summary: string
}

export async function resolveMcpOptions(cwd: string, mode: McpMode): Promise<ResolvedMcpOptions> {
  if (mode === "off") {
    return {
      mode,
      summary: "MCP disabled",
    }
  }

  if (mode === "project") {
    return {
      mode,
      settingSources: ["project"],
      summary: "project settings from .cursor/mcp.json",
    }
  }

  const serverPath = path.join(cwd, "research-mcp", "server.js")
  const serverCwd = path.dirname(serverPath)
  await assertExists(serverPath, "research-mcp/server.js")

  return {
    mode,
    mcpServers: {
      "ai-co-scientist-research": {
        type: "stdio",
        command: "node",
        args: [serverPath],
        cwd: serverCwd,
      },
    },
    summary: "inline stdio server research-mcp/server.js",
  }
}

export function parseMcpMode(value: string): McpMode {
  if (value === "inline" || value === "project" || value === "off") {
    return value
  }
  throw new Error(`Invalid --mcp value: ${value}. Expected inline, project, or off.`)
}

async function assertExists(filePath: string, label: string) {
  try {
    await access(filePath)
  } catch {
    throw new Error(`Cannot enable inline MCP because ${label} is missing at ${filePath}`)
  }
}
