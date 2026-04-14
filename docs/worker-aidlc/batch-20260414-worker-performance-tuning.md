## Batch

- Scope: worker performance tuning
- Date: 2026-04-14

## Changes

- Made `passive_monitor_builder.py` less allocation-heavy by reusing materialized row records instead of repeatedly calling `to_dict(orient="records")`.
- Switched `_enrich_candidate_frame` to iterate with `itertuples()` so large candidate pools create fewer temporary dictionaries.
- Made default worker counts CPU-aware in `consensus_refresh.py`, `delayed_quote_collector.py`, and `passive_monitor_builder.py` so small servers avoid oversubscription and larger servers avoid being underutilized.

## Proof

- `python3 -m unittest Disclosure.tests.test_passive_monitor_builder Disclosure.tests.test_market_warning_monitor_builder Disclosure.tests.test_official_index_clone Disclosure.tests.test_official_index_clone_prepare`
- `python3 -m py_compile Disclosure/passive_monitor_builder.py Disclosure/delayed_quote_collector.py Disclosure/consensus_refresh.py`
- `npm run aidlc:audit:strict`
