# Worker AIDLC

## Goal

This folder tracks the AIDLC guardrails for the worker repo without destabilizing the production data pipeline.

## Phase 1

- Add architecture and workflow docs.
- Add a line-limit audit for operator and orchestration files.
- Add git hooks for local verification.
- Keep `Disclosure/` legacy modules out of the hard gate until they are split intentionally.

## Commands

```bash
npm run aidlc:audit
npm run aidlc:audit:strict
npm run verify:aidlc
npm run hooks:install
```

## Guarded Scope

- `scripts/`
- `ecosystem.config.cjs`
- `README.md`
- `ARCHITECTURE.md`
- `docs/worker-aidlc/`
- `.githooks/`

## Batches

- `batch-20260414-worker-aidlc-bootstrap.md`
