import { mkdir, writeFile } from "node:fs/promises"
import path from "node:path"

export function slugify(value: string) {
  return value
    .toLowerCase()
    .replace(/['"]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 72) || "investigation"
}

export function investigationDir(cwd: string, slug: string) {
  return path.join(cwd, ".co-scientist", "investigations", slug)
}

export async function ensureQuestionFile(cwd: string, slug: string, question: string) {
  const root = investigationDir(cwd, slug)
  await mkdir(root, { recursive: true })
  await writeFile(path.join(root, "question.md"), `${question.trim()}\n`, "utf8")
  return root
}
