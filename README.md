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
- FastAPI 읽기 전용 API 제공

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
- `worker-read-api`
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

macOS / Linux:

```bash
cp .env.example .env
PYTHON_BIN=python3.11 npm run bootstrap
npm run worker:smoke
npm run worker:seed
npm run pm2:start
```

Windows PowerShell:

```powershell
Copy-Item .env.example .env
npm run bootstrap
npm run worker:smoke
npm run worker:seed
npm run pm2:start
```

이 레포는 `worker-only`라서 `npm run build`나 `npm run start`는 없습니다. 상태 확인은 `npm run pm2:status`를 사용합니다.

Windows에서 `No suitable Python runtime found`가 나오면:

```powershell
py -0p
```

- Python이 없다면 Python 3.11 x64를 설치한 뒤 PowerShell을 다시 엽니다.
- Python이 다른 경로에만 잡혀 있다면 아래처럼 직접 지정할 수 있습니다.

```powershell
$env:PYTHON_BIN="C:\Path\To\python.exe"
npm run bootstrap
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
- `READ_API_PORT` 기본값 `8000`

## 웹 서버 연결

`trading-value-web` 쪽은 아래처럼 두면 됩니다.

```bash
READ_MODEL_API_BASE_URL=http://your-worker-host:8000
```

Vercel이나 외부 웹앱이 붙을 때는 `8000` 직접 노출보다 `Caddy -> 127.0.0.1:8000` reverse proxy를 권장합니다.
이 레포에는 바로 쓸 수 있는 [Caddyfile](/Users/mac_mini/Documents/GitHub/Trading/trading-value-worker/Caddyfile) 가 포함되어 있습니다.

기본 Caddyfile은 `:80` 에서 받아서 `worker-read-api` 로 프록시합니다.

```bash
curl http://52.64.85.49/health
curl http://52.64.85.49/api/read-models/dashboard
```

이 구성에서는 웹앱 환경변수를 아래처럼 두면 됩니다.

```bash
READ_MODEL_API_BASE_URL=http://52.64.85.49
```

Windows 서버에서 Caddy를 서비스로 돌리는 예시는 아래와 같습니다.

```powershell
caddy run --config C:\apps\Trading-Server\Caddyfile
```

운영에서는 AWS 보안그룹과 Windows 방화벽에 `80` 포트를 열어 주세요.

읽기 전용 API 상태 확인:

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/api/read-models/dashboard
curl http://127.0.0.1:8000/api/analyst-board/012450
```

## 운영 메모

- 이 레포는 로컬 projection 파일도 같이 씁니다.
  - `Disclosure/runtime/web_projections/*`
- 하지만 운영 기준의 단일 진실 원천은 Mongo를 권장합니다.
- `OPENAI_API_KEY`는 웹 서버에 넣고, 이 워커 레포에는 넣지 않아도 됩니다.
