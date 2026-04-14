## Batch

- Scope: market warning and read api module split
- Date: 2026-04-14

## Changes

- Extracted market warning candidate rules and snapshot assembly into `Disclosure/market_warning_candidates.py`.
- Kept `Disclosure/market_warning_monitor_builder.py` as the scheduler/runtime entrypoint while preserving the public test imports.
- Split `read_api.py` into `read_api_core.py`, `read_api_models.py`, `read_api_quotes.py`, `read_api_analyst.py`, and `read_api_routes.py`.
- Reduced `read_api.py` to a thin app bootstrap so `uvicorn read_api:app` keeps the same public entrypoint.

## Proof

- `python3 -m unittest Disclosure.tests.test_market_warning_monitor_builder`
- `python3 Disclosure/market_warning_monitor_builder.py --once --skip-mongo --print-only`
- `wc -l read_api.py read_api_core.py read_api_models.py read_api_quotes.py read_api_analyst.py read_api_routes.py`
