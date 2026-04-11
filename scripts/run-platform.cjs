const { spawnSync } = require("child_process");
const path = require("path");

const rootDir = path.resolve(__dirname, "..");
const scriptDir = path.join(rootDir, "scripts");
const isWin = process.platform === "win32";

function run(command, args) {
  const result = spawnSync(command, args, {
    cwd: rootDir,
    stdio: "inherit",
    shell: false,
  });
  if (result.error) {
    throw result.error;
  }
  process.exit(result.status ?? 0);
}

const task = process.argv[2];
const extra = process.argv.slice(3);

const taskMap = {
  bootstrap: {
    unix: ["bash", [path.join(scriptDir, "bootstrap.sh")]],
    win: ["powershell.exe", ["-ExecutionPolicy", "Bypass", "-File", path.join(scriptDir, "bootstrap.ps1")]],
  },
  pm2: {
    unix: ["bash", [path.join(scriptDir, "worker_pm2.sh"), ...extra]],
    win: ["powershell.exe", ["-ExecutionPolicy", "Bypass", "-File", path.join(scriptDir, "worker_pm2.ps1"), ...extra]],
  },
  smoke: {
    unix: ["bash", [path.join(scriptDir, "worker_smoke.sh")]],
    win: ["powershell.exe", ["-ExecutionPolicy", "Bypass", "-File", path.join(scriptDir, "worker_smoke.ps1")]],
  },
  seed: {
    unix: ["bash", [path.join(scriptDir, "refresh_now.sh")]],
    win: ["powershell.exe", ["-ExecutionPolicy", "Bypass", "-File", path.join(scriptDir, "refresh_now.ps1")]],
  },
};

if (!taskMap[task]) {
  console.error(`Unknown task: ${task}`);
  process.exit(1);
}

const target = isWin ? taskMap[task].win : taskMap[task].unix;
run(target[0], target[1]);
