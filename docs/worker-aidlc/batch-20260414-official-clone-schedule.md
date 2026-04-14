## Batch

- Scope: official clone modules and worker schedule wiring
- Date: 2026-04-14

## Changes

- Split official clone input loading and snapshot assembly into `Disclosure/official_index_clone_support.py` and `Disclosure/official_index_clone_snapshot.py`.
- Kept `Disclosure/official_index_clone.py` as a thin compatibility export so existing imports continue to work.
- Added canonical input docs and examples under `Disclosure/index_clone_inputs/`.
- Wired passive and market warning jobs into `ecosystem.config.cjs` and both `refresh_now` scripts.

## Proof

- `.venv/bin/python -c "import read_api; print(read_api.app.title)"`
- `python3 -m unittest Disclosure.tests.test_official_index_clone Disclosure.tests.test_official_index_clone_prepare`
- `python3 -m py_compile Disclosure/official_index_clone.py Disclosure/official_index_clone_support.py Disclosure/official_index_clone_snapshot.py Disclosure/official_index_clone_prepare.py`
