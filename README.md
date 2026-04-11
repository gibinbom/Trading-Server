# Trading Value Worker

웹앱과 분리해서 `데이터 갱신`만 담당하는 PM2 워커 레포입니다.  
이 레포는 `Disclosure` 파이프라인을 주기적으로 돌려서 Mongo read model과 로컬 projection 파일을 갱신합니다.

## 역할

- 컨센서스/실적 스냅샷 갱신
- 적정가 스냅샷 재계산
- 지연시세/수급 스냅샷 갱신
- 순환매 히스토리 갱신
- 공시 이벤트 수집
- 웹 projection 발행
- 매크로 뉴스 모니터링

웹 서버는 이 레포를 직접 실행하지 않고, 같은 Mongo를 읽도록 두는 구성이 가장 안정적입니다.

## 권장 아키텍처

1. `trading-value-worker` 서버
   - PM2로 Python updater 프로세스 상시 실행
   - MongoDB에 read model 발행
2. `trading-value-web` 서버
   - Next.js만 실행
   - `READ_MODEL_SOURCE=mongo`
   - 같은 `MONGO_URI`, `DB_NAME` 사용

## 포함한 기본 워커

- `worker-consensus-refresh-full`
- `worker-consensus-refresh-incremental`
- `worker-actual-financial-refresh`
- `worker-fair-value-builder`
- `worker-delayed-quote`
- `worker-flow-snapshot-full`
- `worker-flow-snapshot-incremental`
- `worker-sector-rotation-history`
- `worker-event-collector`
- `worker-web-projection`
- `worker-macro-news`

기본 세트는 `웹에서 바로 보이는 값` 갱신에 집중했습니다.  
KIS 계정 의존도가 높은 실시간 감시기(`wics_monitor`, 자동매매, Slack 리포터)는 일부러 제외했습니다.

## 빠른 시작

```bash
cp .env.example .env
PYTHON_BIN=python3.11 npm run bootstrap
npm run worker:smoke
npm run worker:seed
npm run pm2:start
```

상태 확인:

```bash
npm run pm2:status
./scripts/worker_pm2.sh logs worker-web-projection 100
```

`worker:seed`는 첫 배포 직후 한 번 실행해서, 스케줄 시간을 기다리지 않고 Mongo와 projection을 바로 채우는 용도입니다.

## 필수 환경변수

- `MONGO_URI`
- `DB_NAME`
- `OPEN_DART_API_KEY`

## 선택 환경변수

- `SLACK_WEBHOOK_URL`
- `GEMINI_API_KEY`
- `PLAYWRIGHT_SKIP_BROWSER_INSTALL=1`

## 웹 서버 연결

`trading-value-web` 쪽은 아래처럼 두면 됩니다.

```bash
READ_MODEL_SOURCE=mongo
MONGO_URI=mongodb://your-mongo-host:27017
DB_NAME=stock_data
```

## 운영 메모

- 이 레포는 로컬 projection 파일도 같이 씁니다.
  - `Disclosure/runtime/web_projections/*`
- 하지만 운영 기준의 단일 진실 원천은 Mongo를 권장합니다.
- `OPENAI_API_KEY`는 웹 서버에 넣고, 이 워커 레포에는 넣지 않아도 됩니다.
