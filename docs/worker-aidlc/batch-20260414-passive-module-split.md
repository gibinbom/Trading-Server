## Batch

- Scope: passive monitor module split
- Date: 2026-04-14

## Changes

- Extracted passive cache, projection, public-float, and special-event helpers into `Disclosure/passive_monitor_support.py`.
- Extracted ETF gap helper and builder logic into `Disclosure/passive_etf_gap.py`.
- Reduced `Disclosure/passive_monitor_builder.py` by moving cross-cutting logic into dedicated modules while preserving the public entrypoint and test imports.

## Proof

- `python3 -m unittest Disclosure.tests.test_passive_monitor_builder`
- `python3 Disclosure/passive_monitor_builder.py --once --skip-mongo --print-only --methodology-mode public-faithful`
