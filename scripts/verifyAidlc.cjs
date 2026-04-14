const { execFileSync } = require("node:child_process");

const GUARDED_PATTERNS = [
  /^scripts\//,
  /^README\.md$/,
  /^ARCHITECTURE\.md$/,
  /^ecosystem\.config\.cjs$/,
  /^docs\/worker-aidlc\//,
  /^\.githooks\//,
];

function run(command, args, options = {}) {
  return execFileSync(command, args, {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    ...options,
  }).trim();
}

function hasGuardedChanges(files) {
  return files.some((file) => GUARDED_PATTERNS.some((pattern) => pattern.test(file)));
}

const output = run("git", ["diff", "--cached", "--name-only", "--diff-filter=ACMR"]);
const files = output ? output.split("\n").map((line) => line.trim()).filter(Boolean) : [];
if (!hasGuardedChanges(files)) {
  console.log("[aidlc-verify] no guarded worker files staged; skipping.");
  process.exit(0);
}

console.log("[aidlc-verify] running worker audit...");
execFileSync("npm", ["run", "aidlc:audit:strict"], { stdio: "inherit" });
console.log("[aidlc-verify] passed.");
