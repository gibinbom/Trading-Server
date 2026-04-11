# ls_news.py
import asyncio
import websockets
import json
import requests
import logging

# 설정 파일에서 키 가져오기 (main.py와 공유)
from config import SETTINGS
from stock_news_pipeline import append_stock_news_packet


# 로거 설정
logger = logging.getLogger("LS_NEWS")

# 감지할 핵심 키워드
TARGET_KEYWORDS = [
    "실적", "잠정",
    "수주", "단일판매", "공급계약체결",
    "변경", "30%", "15%"
]

RECONNECT_DELAY = 5 

APP_KEY = "PS9bfUdNbPUNkbEBvYQ99542A55BVVkRkDFO", 
APP_SECRET_KEY = "vMBp4IpMGBgUOdjYn1X9N5M4q4HS8pwi"

def get_access_token():
    """LS증권 Access Token 발급"""
    # config.py의 설정을 사용
    is_sim = getattr(SETTINGS, "IS_SIMULATION", False)
    app_key = getattr(SETTINGS, "LS_APP_KEY", "PS9bfUdNbPUNkbEBvYQ99542A55BVVkRkDFO")       # config.py에 설정된 키 사용
    app_secret = getattr(SETTINGS, "LS_APP_SECRET", "vMBp4IpMGBgUOdjYn1X9N5M4q4HS8pwi") 

    base_url = "https://openapi.ls-sec.co.kr:29443" if is_sim else "https://openapi.ls-sec.co.kr:8080"
    url = f"{base_url}/oauth2/token"
    
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "appkey": app_key,
        "appsecretkey": app_secret,
        "scope": "oob"
    }

    try:
        resp = requests.post(url, headers=headers, data=data, timeout=5)
        if resp.status_code == 200:
            return resp.json().get("access_token")
        else:
            logger.error(f"토큰 발급 실패: {resp.text}")
            return None
    except Exception as e:
        logger.error(f"토큰 요청 에러: {e}")
        return None

def analyze_news_packet(body):
    """뉴스 패킷 분석 -> 키워드 감지"""
    title = body.get("title", "")
    code = body.get("code", "")
    time_str = body.get("time", "")

    # 키워드 매칭
    hit_keyword = next((k for k in TARGET_KEYWORDS if k in title), None)

    if hit_keyword:
        logger.warning(f"🚨 [SIGNAL DETECTED] 키워드: {hit_keyword} | 제목: {title}")
        return True, title
    
    return False, None

async def connect_and_listen(on_signal_callback=None):
    """
    웹소켓 연결 및 수신 루프
    Args:
        on_signal_callback: 시그널 감지 시 실행할 함수
    """
    is_sim = getattr(SETTINGS, "IS_SIMULATION", False)
    
    while True: # 재접속 루프
        token = get_access_token()
        if not token:
            logger.error("토큰 발급 실패. 대기 후 재시도...")
            await asyncio.sleep(10)
            continue

        ws_url = "wss://openapi.ls-sec.co.kr:29443/websocket" if is_sim else "wss://openapi.ls-sec.co.kr:9443/websocket"
        logger.info(f"뉴스 서버 연결 시도: {ws_url}")

        try:
            async with websockets.connect(ws_url) as websocket:
                # 등록 요청
                req_packet = {
                    "header": {"token": token, "tr_type": "3"},
                    "body": {"tr_cd": "NWS", "tr_key": "NWS001"}
                }
                await websocket.send(json.dumps(req_packet))
                logger.info("✅ 뉴스 감지 시작 (Listening...)")

                while True:
                    msg = await websocket.recv()
                    data = json.loads(msg)
                    body = data.get("body")

                    if body and "title" in body:
                        # Trigger 여부와 별개로 원시 종목 뉴스를 모두 적재해 둡니다.
                        # 나중에 종목별 뉴스 스코어링/백테스트에 재활용할 수 있습니다.
                        append_stock_news_packet(body, source="LS_WEBSOCKET")

                        # ==========================================================
                        # 🔥 [핵심 수정] ID가 "15"가 아니면 즉시 무시 (Skip)
                        # ==========================================================
                        news_id = str(body.get("id", "")) # 문자열로 변환하여 비교
                        if news_id != "15":
                            # ID 15가 아니면 로그도 찍지 않고 넘어감 (노이즈 제거)
                            continue

                        # ID 15인 경우에만 키워드 분석 진행
                        is_hit, title = analyze_news_packet(body)
                        
                        if is_hit and on_signal_callback:
                            logger.info(f"⚡ [ID:{news_id}] 공시성 뉴스 감지! Triggering Burst Poll...")
                            
                            # 콜백 실행 (Blocking 방지 처리)
                            if asyncio.iscoroutinefunction(on_signal_callback):
                                await on_signal_callback()
                            else:
                                on_signal_callback() 
                                
                    elif data.get("header", {}).get("tr_cd") == "NWS" and body is None:
                        pass # 등록 확인 메시지

        except websockets.exceptions.ConnectionClosed:
            logger.warning(f"서버 연결 끊김. {RECONNECT_DELAY}초 후 재접속...")
        except Exception as e:
            logger.error(f"웹소켓 에러: {e}")
        
        await asyncio.sleep(RECONNECT_DELAY)
