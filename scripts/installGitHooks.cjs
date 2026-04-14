const { execFileSync } = require("node:child_process");
const path = require("node:path");

const repoRoot = path.resolve(__dirname, "..");
const hooksPath = path.join(repoRoot, ".githooks");

function run(command, args) {
  execFileSync(command, args, {
    cwd: repoRoot,
    stdio: "inherit",
  });
}

run("git", ["config", "core.hooksPath", hooksPath]);
console.log(`[hooks] core.hooksPath -> ${hooksPath}`);
