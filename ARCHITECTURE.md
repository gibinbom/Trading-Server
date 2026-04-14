# Worker Architecture

## Purpose

`trading-value-worker` is the data-side runtime for Trading. It refreshes market data, builds read-model snapshots, publishes projection files, and serves the read-only API consumed by `trading-value-web`.

## AIDLC Scope

Phase 1 AIDLC coverage applies to:

- `scripts/`
- `ecosystem.config.cjs`
- `README.md`
- `ARCHITECTURE.md`
- `docs/worker-aidlc/`
- `.githooks/`

Large Python pipeline modules in `Disclosure/` remain legacy application code for now. They stay outside the initial hard line-limit gate until they are split by domain in follow-up batches.

## Layers

1. Operator Layer
   - `README.md`
   - `ARCHITECTURE.md`
   - `docs/worker-aidlc/`
   - `.githooks/`

2. Runtime Orchestration
   - `package.json`
   - `scripts/run-platform.cjs`
   - `scripts/bootstrap.*`
   - `scripts/refresh_now.*`
   - `ecosystem.config.cjs`

3. Worker Processes
   - `Disclosure/*_builder.py`
   - `Disclosure/*_collector.py`
   - `Disclosure/*_refresh.py`
   - `Disclosure/web_projection_publisher.py`

4. Read Models / Serving
   - `read_api.py`
   - `Disclosure/runtime/web_projections/*`
   - Mongo read models

## Principles

- Route operational changes through small scripts and docs first.
- Keep shell and orchestration files short and composable.
- Do not block urgent market-data fixes on full pipeline refactors.
- Split large `Disclosure/` modules by domain with proof batches rather than big-bang rewrites.

## Current Follow-ups

- Phase 2: introduce worker smoke proof docs and staged companion enforcement for runtime scripts.
- Phase 3: split oversized `Disclosure/` modules starting with `passive_monitor_builder.py`, `market_warning_monitor_builder.py`, and `web_projection_publisher.py`.
