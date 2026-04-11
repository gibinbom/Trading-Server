import os
import json
import requests
from datetime import datetime, timedelta

try:
    from runtime_paths import KIS_TOKEN_FILE, LEGACY_KIS_TOKEN_FILES, ensure_runtime_dir
except Exception:
    from Disclosure.runtime_paths import KIS_TOKEN_FILE, LEGACY_KIS_TOKEN_FILES, ensure_runtime_dir


TOKEN_FILE = KIS_TOKEN_FILE
TOKEN_HTTP_TIMEOUT_SEC = 10


def _load_cached_tokens() -> dict:
    for path in [TOKEN_FILE, *LEGACY_KIS_TOKEN_FILES]:
        if not path or not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if path != TOKEN_FILE and payload:
                try:
                    ensure_runtime_dir()
                    with open(TOKEN_FILE, "w", encoding="utf-8") as out:
                        json.dump(payload, out, indent=4)
                except Exception:
                    pass
            return payload
        except Exception:
            continue
    return {}

def get_valid_tokens(appkey, appsecret, is_virtual=False):
    """
    로컬에 저장된 토큰을 확인하고, 유효하면 그대로 반환합니다.
    만료되었거나 없으면 새로 발급받아 파일에 저장합니다. (안전빵 23시간 기준)
    """
    base_url = 'https://openapivts.koreainvestment.com:29443' if is_virtual else 'https://openapi.koreainvestment.com:9443'
    
    tokens = {}
    now = datetime.now()

    # 1. 파일에서 기존 토큰 읽어오기
    tokens = _load_cached_tokens()

    # 2. REST API 토큰 (만료 24시간 -> 여유분 1시간 빼서 23시간으로 체크)
    need_new_rest = True
    if "rest_token" in tokens and "rest_expire" in tokens:
        expire_time = datetime.strptime(tokens["rest_expire"], "%Y-%m-%d %H:%M:%S")
        if now < expire_time:
            need_new_rest = False

    if need_new_rest:
        print("🔄 [Token Manager] REST API 토큰 신규 발급 중...")
        url = f"{base_url}/oauth2/tokenP"
        body = {"grant_type": "client_credentials", "appkey": appkey, "appsecret": appsecret}
        try:
            response = requests.post(url, json=body, timeout=TOKEN_HTTP_TIMEOUT_SEC)
            response.raise_for_status()
            res = response.json()
        except Exception as exc:
            res = {"error": str(exc)}
        
        if "access_token" in res:
            tokens["rest_token"] = res["access_token"]
            tokens["rest_expire"] = (now + timedelta(hours=23)).strftime("%Y-%m-%d %H:%M:%S")
        else:
            print(f"🚨 REST 토큰 발급 실패: {res}")

    # 3. Websocket Approval Key 발급
    need_new_ws = True
    if "ws_approval_key" in tokens and "ws_expire" in tokens:
        expire_time = datetime.strptime(tokens["ws_expire"], "%Y-%m-%d %H:%M:%S")
        if now < expire_time:
            need_new_ws = False

    if need_new_ws:
        print("🔄 [Token Manager] Websocket Approval Key 신규 발급 중...")
        url = f"{base_url}/oauth2/Approval"
        body = {"grant_type": "client_credentials", "appkey": appkey, "secretkey": appsecret}
        try:
            response = requests.post(url, json=body, timeout=TOKEN_HTTP_TIMEOUT_SEC)
            response.raise_for_status()
            res = response.json()
        except Exception as exc:
            res = {"error": str(exc)}
        
        if "approval_key" in res:
            tokens["ws_approval_key"] = res["approval_key"]
            tokens["ws_expire"] = (now + timedelta(hours=23)).strftime("%Y-%m-%d %H:%M:%S")
        else:
            print(f"🚨 웹소켓 키 발급 실패: {res}")

    # 4. 갱신된 내역 파일에 저장
    if need_new_rest or need_new_ws:
        ensure_runtime_dir()
        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            json.dump(tokens, f, indent=4)

    return tokens.get("rest_token"), tokens.get("ws_approval_key")
