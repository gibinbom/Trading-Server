const fs = require("fs");
const path = require("path");
const dotenv = require("dotenv");

const rootDir = __dirname;
const envPath = path.join(rootDir, ".env");
if (fs.existsSync(envPath)) {
  dotenv.config({ path: envPath });
}

const defaultPythonBin = process.platform === "win32"
  ? path.join(rootDir, ".venv", "Scripts", "python.exe")
  : path.join(rootDir, ".venv", "bin", "python");

const pythonBin = process.env.WORKER_PYTHON_BIN || defaultPythonBin;
const readApiPort = process.env.READ_API_PORT || "8000";

const delayedQuoteTimes = (() => {
  const slots = ["08:15"];
  for (let hour = 9; hour <= 15; hour += 1) {
    for (let minute = 0; minute < 60; minute += 5) {
      if (hour === 9 && minute < 5) continue;
      if (hour === 15 && minute > 45) continue;
      slots.push(`${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`);
    }
  }
  slots.push("20:15");
  return slots.join(",");
})();

const baseApp = {
  cwd: rootDir,
  script: pythonBin,
  autorestart: true,
  restart_delay: 5000,
  time: true,
  max_memory_restart: "1G",
  env: {
    PYTHONUNBUFFERED: process.env.PYTHONUNBUFFERED || "1",
    PYTHONUTF8: process.env.PYTHONUTF8 || "1",
    PYTHONIOENCODING: process.env.PYTHONIOENCODING || "utf-8",
    TZ: process.env.TZ || "Asia/Seoul",
    MONGO_URI: process.env.MONGO_URI || "mongodb://127.0.0.1:27017",
    DB_NAME: process.env.DB_NAME || "stock_data",
    OPEN_DART_API_KEY: process.env.OPEN_DART_API_KEY || "",
    SLACK_WEBHOOK_URL: process.env.SLACK_WEBHOOK_URL || "",
    GEMINI_API_KEY: process.env.GEMINI_API_KEY || "",
    WS_MAX_WORKERS: process.env.WS_MAX_WORKERS || "1",
    WICS_CORE_LIMIT: process.env.WICS_CORE_LIMIT || "12",
    WICS_DYNAMIC_LIMIT: process.env.WICS_DYNAMIC_LIMIT || "4",
    WICS_TOTAL_LIMIT: process.env.WICS_TOTAL_LIMIT || "16",
    WICS_DYNAMIC_MIN_SCORE: process.env.WICS_DYNAMIC_MIN_SCORE || "0.54",
    WICS_DYNAMIC_MIN_SOURCES: process.env.WICS_DYNAMIC_MIN_SOURCES || "2",
    FLOW_SNAPSHOT_INTERVAL_SEC: process.env.FLOW_SNAPSHOT_INTERVAL_SEC || "180",
    FLOW_SNAPSHOT_MIN_GROSS_AMT_MIL: process.env.FLOW_SNAPSHOT_MIN_GROSS_AMT_MIL || "50",
    FLOW_SNAPSHOT_FORCE_GROSS_AMT_MIL: process.env.FLOW_SNAPSHOT_FORCE_GROSS_AMT_MIL || "300",
    READ_API_PORT: readApiPort
  }
};

module.exports = {
  apps: [
    {
      ...baseApp,
      name: "worker-read-api",
      args: `-m uvicorn read_api:app --host 0.0.0.0 --port ${readApiPort}`
    },
    {
      ...baseApp,
      name: "worker-consensus-refresh-full",
      args: "Disclosure/consensus_refresh.py --mode full --times 06:40"
    },
    {
      ...baseApp,
      name: "worker-consensus-refresh-incremental",
      args: "Disclosure/consensus_refresh.py --mode incremental --times 11:40,15:25,20:05"
    },
    {
      ...baseApp,
      name: "worker-actual-financial-refresh",
      args: "Disclosure/actual_financial_refresh.py --times 06:45,11:45,15:30,20:10"
    },
    {
      ...baseApp,
      name: "worker-fair-value-builder",
      args: "Disclosure/fair_value_builder.py --times 06:55,11:55,15:40,20:20 --top-n 20"
    },
    {
      ...baseApp,
      name: "worker-delayed-quote",
      args: `Disclosure/delayed_quote_collector.py --times ${delayedQuoteTimes}`
    },
    {
      ...baseApp,
      name: "worker-flow-snapshot-full",
      args: "Disclosure/flow_snapshot_builder.py --mode full --disable-kis --times 06:35,20:05"
    },
    {
      ...baseApp,
      name: "worker-flow-snapshot-incremental",
      args: "Disclosure/flow_snapshot_builder.py --mode incremental --disable-kis --times 11:35,15:20"
    },
    {
      ...baseApp,
      name: "worker-sector-rotation-history",
      args: "Disclosure/sector_rotation_history_builder.py --weeks 52 --times 06:58,11:58,15:43,20:23"
    },
    {
      ...baseApp,
      name: "worker-event-collector",
      args: "Disclosure/disclosure_event_collector.py --max-pages 2 --poll-sec 30 --off-hours-poll-sec 300 --backfill-days 45 --markets KOSPI,KOSDAQ"
    },
    {
      ...baseApp,
      name: "worker-web-projection",
      args: "Disclosure/web_projection_publisher.py --times 07:00,12:00,15:45,20:25"
    },
    {
      ...baseApp,
      name: "worker-macro-news",
      args: "Disclosure/signals/macro_news_monitor.py"
    }
  ]
};
