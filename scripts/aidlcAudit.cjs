const fs = require("node:fs/promises");
const path = require("node:path");

const repoRoot = path.resolve(__dirname, "..");
const strict = process.argv.includes("--strict");
const targetLines = Number.parseInt(process.env.AIDLC_TARGET_LINES || "200", 10);
const hardLines = Number.parseInt(process.env.AIDLC_HARD_LINES || "250", 10);
const entries = [
  "scripts",
  "README.md",
  "ARCHITECTURE.md",
  "ecosystem.config.cjs",
  "docs/worker-aidlc",
  ".githooks",
];
const allowedExtensions = new Set([".md", ".js", ".cjs", ".sh", ".ps1"]);

async function statSafe(filePath) {
  try {
    return await fs.stat(filePath);
  } catch {
    return null;
  }
}

async function collect(entry) {
  const absolutePath = path.join(repoRoot, entry);
  const stat = await statSafe(absolutePath);
  if (!stat) return [];
  if (stat.isFile()) return [absolutePath];
  const files = [];
  const stack = [absolutePath];
  while (stack.length > 0) {
    const current = stack.pop();
    const children = await fs.readdir(current, { withFileTypes: true });
    for (const child of children) {
      const childPath = path.join(current, child.name);
      if (child.isDirectory()) {
        stack.push(childPath);
        continue;
      }
      if (allowedExtensions.has(path.extname(child.name)) || child.name === "pre-commit" || child.name === "pre-push") {
        files.push(childPath);
      }
    }
  }
  return files;
}

async function main() {
  const allFiles = new Set();
  for (const entry of entries) {
    for (const file of await collect(entry)) allFiles.add(file);
  }
  const results = [];
  for (const file of [...allFiles].sort()) {
    const relative = path.relative(repoRoot, file);
    const contents = await fs.readFile(file, "utf8");
    const lineCount = contents.length === 0 ? 0 : contents.split(/\r?\n/).length;
    if (lineCount <= targetLines) continue;
    results.push({
      file: relative,
      lineCount,
      severity: lineCount > hardLines ? "HARD" : "WARN",
    });
  }
  console.log(`[aidlc] target=${targetLines} hard=${hardLines}`);
  if (results.length === 0) {
    console.log("[aidlc] No files exceeded the configured target.");
    return;
  }
  const hardCount = results.filter((item) => item.severity === "HARD").length;
  for (const item of results) {
    console.log(`[aidlc] ${item.severity} ${String(item.lineCount).padStart(4, " ")}  ${item.file}`);
  }
  console.log(`[aidlc] ${results.length} file(s) exceeded ${targetLines} lines; ${hardCount} file(s) exceeded ${hardLines} lines.`);
  if (strict) process.exit(1);
}

main().catch((error) => {
  console.error("[aidlc] failed", error);
  process.exit(1);
});
