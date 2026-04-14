# Official Index Clone Inputs

`PASSIVE_INDEX_METHODOLOGY_MODE=official` 로 실행할 때 사용하는 공식/라이선스 입력 디렉터리입니다.

현재 구현은 `KS200`, `KQ150` 전용입니다. 이 모드는 proxy 계산을 쓰지 않고, 아래 CSV 3개가 모두 있어야 동작합니다.

## Required files

### `reviews.csv`

리밸런싱 심사 배치를 정의합니다.

필수 컬럼:

- `review_date`
- `index_name`
- `effective_date`
- `cutoff`
- `entry_ratio`
- `keep_ratio`
- `liquidity_coverage`
- `special_largecap_rank`

### `universe.csv`

심사 기준일 기준 공식/라이선스 모집단 데이터입니다.

필수 컬럼:

- `review_date`
- `index_name`
- `symbol`
- `name`
- `market`
- `official_sector`
- `official_bucket`
- `avg_ffmc_1y_krw`
- `avg_trading_value_1y_krw`
- `market_cap_rank_all`
- `listing_age_days`
- `free_float_ratio`
- `is_eligible`
- `is_current_member`

권장 컬럼:

- `eligibility_reason`
- `security_type`
- `reserve_rank`

### `bucket_targets.csv`

심사 배치별 공식 버킷 목표 좌석 수입니다.

필수 컬럼:

- `review_date`
- `index_name`
- `official_bucket`
- `target_count`

## Notes

- `avg_ffmc_1y_krw` 는 free-float adjusted 1년 일평균 시가총액이어야 합니다.
- `avg_trading_value_1y_krw` 는 정기심사 기준 1년 일평균 거래대금이어야 합니다.
- `official_bucket` 은 공식 방법론에 맞는 산업군/섹터 체계를 따라야 합니다.
- 현재 엔진은 이 입력을 바탕으로 정기변경용 clone snapshot을 만듭니다.
- 특별변경, 예비종목, 신규상장 특례, 합병/분할 교체를 완전 복제하려면 별도 action/event 입력이 추가로 필요합니다.

## Run

```bash
cd trading-value-worker
PASSIVE_INDEX_METHODOLOGY_MODE=official \
python Disclosure/passive_monitor_builder.py --once --skip-mongo --print-only
```

## Raw vendor export normalization

원본 추출본 컬럼명이 제각각이면 아래 준비 스크립트로 canonical CSV를 만들 수 있습니다.

기본 raw 파일 경로:

- `Disclosure/index_clone_inputs/raw/review_metadata.csv`
- `Disclosure/index_clone_inputs/raw/universe_export.csv`
- `Disclosure/index_clone_inputs/raw/bucket_targets_export.csv`

정규화 실행:

```bash
cd trading-value-worker
python Disclosure/official_index_clone_prepare.py
```

dry-run:

```bash
cd trading-value-worker
python Disclosure/official_index_clone_prepare.py --print-only
```

현재 스크립트가 인식하는 대표 alias 예시는 아래와 같습니다.

- reviews
  - `심사기준일` -> `review_date`
  - `지수명` -> `index_name`
  - `적용일` -> `effective_date`
- universe
  - `종목코드` -> `symbol`
  - `공식산업군` -> `official_sector`
  - `공식버킷` -> `official_bucket`
  - `1년평균유동시총` -> `avg_ffmc_1y_krw`
  - `1년평균거래대금` -> `avg_trading_value_1y_krw`
  - `유동주식비율` -> `free_float_ratio`
- bucket targets
  - `목표좌석수` -> `target_count`

alias가 맞지 않는 vendor extract면 `official_index_clone_prepare.py`의 alias table에 컬럼명만 추가하면 됩니다.
