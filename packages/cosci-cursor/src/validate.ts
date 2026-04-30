import { spawn } from "node:child_process"
import path from "node:path"
import { investigationDir } from "./paths.js"
import type { Stage } from "./prompts.js"

export type ValidationResult = {
  ok: boolean
  command: string
  code: number | null
  stdout: string
  stderr: string
}

export async function validateInvestigation(cwd: string, slug: string, stage: Stage) {
  const python = process.env.PYTHON || "python3"
  const artifactRoot = investigationDir(cwd, slug)
  const args = [
    "scripts/validate_investigation_artifacts.py",
    artifactRoot,
    "--stage",
    stage,
  ]
  const command = `${python} ${args.map(shellish).join(" ")}`

  return new Promise<ValidationResult>((resolve, reject) => {
    const child = spawn(python, args, {
      cwd,
      stdio: ["ignore", "pipe", "pipe"],
    })
    let stdout = ""
    let stderr = ""

    child.stdout.setEncoding("utf8")
    child.stderr.setEncoding("utf8")
    child.stdout.on("data", (chunk) => {
      stdout += chunk
    })
    child.stderr.on("data", (chunk) => {
      stderr += chunk
    })
    child.on("error", reject)
    child.on("close", (code) => {
      resolve({
        ok: code === 0,
        command,
        code,
        stdout,
        stderr,
      })
    })
  })
}

export function validationOutput(result: ValidationResult) {
  return [
    `$ ${result.command}`,
    result.stdout.trim(),
    result.stderr.trim(),
  ]
    .filter(Boolean)
    .join("\n\n")
}

function shellish(value: string) {
  if (/^[A-Za-z0-9_./:=+-]+$/.test(value)) {
    return value
  }
  return JSON.stringify(value)
}
