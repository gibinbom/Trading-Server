"""
broker_kis.py

한국투자증권(KIS) OpenAPI 연동 모듈 (Smart Routing & Auto-Token Refresh Ver.)
- 403 Forbidden (토큰 만료) 발생 시, 자동으로 기존 토큰 삭제 후 재발급 및 재요청
- 시스템 시간대와 무관하게 KST(한국표준시) 강제 적용
- 시간대별 거래소(KRX/NXT) 및 주문유형(01/03/00) 정밀 자동 전환
"""

import os
import time
import json
import requests
import logging
import datetime as dt
from dataclasses import dataclass
from typing import Optional, Dict, List

try:
    from naver_price_fallback import fetch_naver_quote_snapshot
    from runtime_paths import KIS_TOKEN_FILE
    from token_manager import get_valid_tokens
except Exception:
    from Disclosure.naver_price_fallback import fetch_naver_quote_snapshot
    from Disclosure.runtime_paths import KIS_TOKEN_FILE
    from Disclosure.token_manager import get_valid_tokens

log = logging.getLogger("broker.kis")

# ✅ KST 타임존 정의
KST = dt.timezone(dt.timedelta(hours=9))

@dataclass
class OrderResult:
    ok: bool
    msg: str
    raw: Optional[Dict] = None

# -------------------------------------------------------------------------
# Helper Functions
# -------------------------------------------------------------------------

def _safe_json(resp: requests.Response) -> Optional[Dict]:
    try:
        return resp.json()
    except Exception:
        return None

def _summarize_http(resp: requests.Response) -> tuple[str, dict]:
    j = _safe_json(resp)
    rid = resp.headers.get("x-request-id") or resp.headers.get("X-Request-Id")
    info = {"status": resp.status_code, "request_id": rid}
    if j:
        info.update({"rt_cd": j.get("rt_cd"), "msg1": j.get("msg1")})
        return f"http_fail json={json.dumps(j, ensure_ascii=False)[:200]}", info
    return f"http_fail text={resp.text[:200]}", info


# -------------------------------------------------------------------------
# KIS Broker Class
# -------------------------------------------------------------------------

class KISBroker:
    def __init__(
        self,
        appkey: str,
        appsecret: str,
        cano: str,
        acnt_prdt_cd: str,
        is_virtual: bool = False,
        dry_run: bool = False,
        base_live: str = "https://openapi.koreainvestment.com:9443",
        base_vts: str  = "https://openapivts.koreainvestment.com:29443",
        token_file: str = KIS_TOKEN_FILE
    ):
        self.appkey = appkey
        self.appsecret = appsecret
        self.cano = cano
        self.acnt_prdt_cd = acnt_prdt_cd
        self.is_virtual = is_virtual
        self.dry_run = dry_run
        self.base = base_vts if is_virtual else base_live
        self.token_file = token_file
        
        self._token: Optional[str] = None
        self._sess = requests.Session()

    def _reinitialize_session(self):
        """세션 및 헤더 초기화 (네트워크 오류 시 호출)"""
        try:
            self._sess.close()
        except: pass
        self._sess = requests.Session()
        log.info("✅ [KIS-SESSION] 세션(Session) 재설정 완료")

    def _clear_token_cache(self):
        """메모리 토큰 초기화 (403 발생 시 호출)"""
        # 🚨 [수정 완료] 파일을 통째로 날리면 웹소켓 키도 날아가므로 파일 삭제 로직 제거!
        # 메모리에 들고 있던 토큰만 지워줍니다.
        self._token = None
        log.warning("🗑️ [KIS-TOKEN] 메모리 토큰 초기화 완료 (재요청 시 공용 매니저가 처리함)")

    def _ensure_token(self) -> str:
        # 1. 메모리(self._token)에 토큰이 이미 세팅되어 있다면 즉시 사용
        if self._token:
            return self._token
        
        # 2. 🚨 [수정 완료] 자체 발급 로직을 전부 지우고, '공용 토큰 매니저'에게 위임!
        log.info("🔑 [KIS-TOKEN] 공용 토큰 매니저에서 토큰을 요청합니다...")
        try:
            rest_token, _ = get_valid_tokens(self.appkey, self.appsecret, self.is_virtual)
            
            if not rest_token:
                raise ValueError("공용 매니저로부터 REST 토큰을 받지 못했습니다.")
                
            self._token = rest_token
            return self._token
            
        except Exception as e:
            log.error(f"🚨 [KIS-TOKEN] 공용 매니저 연동 실패: {e}")
            raise
        
    def _headers(self, tr_id: str) -> dict:
        return {
            "authorization": f"Bearer {self._ensure_token()}",
            "appkey": self.appkey,
            "appsecret": self.appsecret,
            "tr_id": tr_id,
            "custtype": "P",
            "content-type": "application/json; charset=utf-8",
        }

    # =========================================================================
    # 🛡️ [CORE] API 호출 통합 래퍼 (403 자동 대응 핵심 로직)
    # =========================================================================
    def _call_api_with_retry(self, method: str, url: str, tr_id: str, params=None, json_data=None):
        """
        API 호출 중 403(Forbidden) 발생 시 토큰을 폐기하고 재발급받아 재시도합니다.
        네트워크 에러 발생 시에도 세션을 복구하고 재시도합니다.
        """
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # 헤더 생성 (여기서 토큰이 없으면 발급받음)
                headers = self._headers(tr_id)
                
                if method == "GET":
                    resp = self._sess.get(url, headers=headers, params=params, timeout=5)
                else:
                    resp = self._sess.post(url, headers=headers, json=json_data, timeout=5)
                
                # 🛑 403 Forbidden or 401 Unauthorized 감지
                if resp.status_code == 403 or resp.status_code == 401:
                    log.warning(f"🚨 [KIS-AUTH] {resp.status_code} 인증 오류! 토큰 삭제 후 재발급 시도 ({attempt+1}/{max_retries})")
                    self._clear_token_cache()     # 기존 토큰 삭제
                    self._reinitialize_session()  # 세션 초기화
                    time.sleep(0.5)
                    continue # 다음 루프에서 새 토큰으로 시도
                
                # 그 외 HTTP 에러 체크
                resp.raise_for_status()
                return resp

            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.ChunkedEncodingError) as e:
                log.warning(f"⚠️ [KIS-NET] 연결 불안정 ({str(e)[:100]}). 재접속 중... ({attempt+1}/{max_retries})")
                self._reinitialize_session()
                time.sleep(0.5)
                continue
            
            except Exception as e:
                # 기타 에러는 바로 throw
                raise e
        
        raise ConnectionError("[KIS] Max retries exceeded")

    # -------------------------------------------------------------------------
    # Market Data Methods
    # -------------------------------------------------------------------------

    def get_last_price(self, symbol: str) -> Optional[float]:
        ctx = self._get_market_context()
        mkt_code = "NX" if ctx["exch"] == "NXT" else "J"
        primary_error: Optional[Exception] = None
        
        tr_id = "FHKST01010100"
        url = f"{self.base}/uapi/domestic-stock/v1/quotations/inquire-price"
        params = {
            "fid_cond_mrkt_div_code": mkt_code,
            "fid_input_iscd": symbol
        }

        try:
            # _call_api_with_retry 사용
            resp = self._call_api_with_retry("GET", url, tr_id, params=params)
            data = resp.json()
            pr = data.get("output", {}).get("stck_prpr")
            if pr is not None:
                return float(str(pr).replace(",", "").strip())
        except Exception as e:
            primary_error = e

        try:
            snapshot = fetch_naver_quote_snapshot(symbol)
            price = snapshot.get("price")
            if price is not None:
                if primary_error is not None:
                    log.info("[KIS][FALLBACK] get_last_price switched to Naver: %s -> %s | primary=%s", symbol, price, str(primary_error)[:120])
                return float(price)
        except Exception as e:
            if primary_error is not None:
                log.warning(f"[KIS] get_last_price failed and fallback also failed: {symbol} | primary={primary_error} | fallback={e}")
            else:
                log.warning(f"[KIS][FALLBACK] get_last_price failed: {symbol} | {e}")

        if primary_error is not None:
            log.warning(f"[KIS] get_last_price failed: {symbol} | {primary_error}")
        return None

    def get_balance(self, **kwargs) -> Optional[Dict]:
        nxt_flag = kwargs.get("afhr_flpr_yn", "N")
        url = f"{self.base}/uapi/domestic-stock/v1/trading/inquire-balance"
        tr_id = "VTTC8434R" if self.is_virtual else "TTTC8434R"
        
        params = {
            "CANO": self.cano,
            "ACNT_PRDT_CD": self.acnt_prdt_cd,
            "AFHR_FLPR_YN": nxt_flag,
            "OFL_YN": "",
            "INQR_DVSN": "02",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "00",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": ""
        }

        try:
            resp = self._call_api_with_retry("GET", url, tr_id, params=params)
            j = resp.json()
            
            if j.get("rt_cd") != "0":
                log.error(f"[KIS] get_balance api fail: {j.get('msg1')}")
                return None

            output1 = j.get("output1", [])
            output2 = j.get("output2", [])

            cash_balance = 0
            if output2:
                cash_balance = int(output2[0].get("dnca_tot_amt", "0"))

            holdings = []
            for item in output1:
                qty = int(item.get("hldg_qty", "0"))
                if qty > 0:
                    holdings.append({
                        "symbol": item.get("pdno"),
                        "name": item.get("prdt_name"),
                        "qty": qty,
                        "avg_price": float(item.get("pchs_avg_pric", "0")),
                        "cur_price": float(item.get("prpr", "0")),
                        "profit_rate": float(item.get("evlu_pfls_rt", "0"))
                    })

            return {"cash": cash_balance, "stocks": holdings}

        except Exception as e:
            log.error(f"[KIS] get_balance exception: {e}")
            return None

    def get_positions(self, **kwargs):
        return self.get_balance(**kwargs)

    def get_sellable_qty(self, symbol: str, **kwargs) -> int:
        if self.is_virtual: return 0 
        tr_id = "TTTC8408R"
        url = f"{self.base}/uapi/domestic-stock/v1/trading/inquire-psbl-sell"
        params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "PDNO": symbol}

        try:
            resp = self._call_api_with_retry("GET", url, tr_id, params=params)
            j = resp.json()
            if j.get("rt_cd") == "0":
                return int(j.get("output", {}).get("ord_psbl_qty", "0"))
            return 0
        except:
            return 0

    # ---------------------------------------------------------------------
    # Order Methods
    # ---------------------------------------------------------------------

    def _get_market_context(self) -> dict:
        now = dt.datetime.now(KST).time()
        if dt.time(8, 0) <= now < dt.time(8, 50):
            return {"exch": "NXT", "type": "03", "manual_price": False, "desc": "NXT-Morn"}
        elif dt.time(15, 30) <= now < dt.time(20, 0):
            return {"exch": "NXT", "type": "00", "manual_price": True, "desc": "NXT-Eve"}
        else:
            return {"exch": "KRX", "type": "01", "manual_price": False, "desc": "KRX-Reg"}

    def buy_market(self, symbol: str, qty: int) -> OrderResult:
        ctx = self._get_market_context()
        price = "0"
        if ctx["manual_price"]:
            current_price = self.get_last_price(symbol)
            if not current_price: return OrderResult(False, "Price fetch error")
            price = str(int(current_price))
        return self._order_cash(symbol, qty, "BUY", ctx["type"], ctx["exch"], price=price)

    def sell_market(self, symbol: str, qty: int) -> OrderResult:
        ctx = self._get_market_context()
        price = "0"
        if ctx["manual_price"]:
            current_price = self.get_last_price(symbol)
            if not current_price: return OrderResult(False, "Price fetch error")
            price = str(int(current_price))
        return self._order_cash(symbol, qty, "SELL", ctx["type"], ctx["exch"], price=price)

    def buy_limit(self, symbol: str, qty: int, price: float) -> OrderResult:
        ctx = self._get_market_context()
        return self._order_cash(symbol, qty, "BUY", "00", ctx["exch"], price=str(int(price)))

    def sell_limit(self, symbol: str, qty: int, price: float) -> OrderResult:
        ctx = self._get_market_context()
        return self._order_cash(symbol, qty, "SELL", "00", ctx["exch"], price=str(int(price)))

    def _order_cash(self, symbol: str, qty: int, side: str, order_type: str, exchange: str, price: str = "0") -> OrderResult:
        if qty <= 0: return OrderResult(False, "qty<=0")
        if self.dry_run:
            msg = f"[DRY_RUN] {side} {symbol} x{qty}"
            log.info(msg)
            return OrderResult(True, msg)

        url = f"{self.base}/uapi/domestic-stock/v1/trading/order-cash"
        body = {
            "CANO": self.cano,
            "ACNT_PRDT_CD": self.acnt_prdt_cd,
            "PDNO": symbol,
            "ORD_DVSN": order_type,       
            "ORD_QTY": str(qty),
            "ORD_UNPR": str(price),
            "EXCG_ID_DVSN_CD": exchange,
        }
        tr_id = "VTTC0012U" if self.is_virtual else ("TTTC0012U" if side == "BUY" else "TTTC0011U")

        try:
            # _call_api_with_retry 사용
            resp = self._call_api_with_retry("POST", url, tr_id, json_data=body)
            j = _safe_json(resp) or {}
            
            if str(j.get("rt_cd")) != "0":
                return OrderResult(False, f"api_fail {j.get('msg1')}", raw=j)
            
            return OrderResult(True, j.get("msg1", "ok"), raw=j)

        except Exception as e:
            return OrderResult(False, f"error: {e}")
        
    def get_price_change_rate(self, symbol: str) -> Optional[float]:
        """
        현재 종목의 전일 대비 등락률(%)을 조회합니다.
        (자사주 소각 등 공시 시점의 선반영 여부 확인용)
        """
        ctx = self._get_market_context()
        mkt_code = "NX" if ctx["exch"] == "NXT" else "J"
        primary_error: Optional[Exception] = None
        
        tr_id = "FHKST01010100" # 주식현재가 시세 API
        url = f"{self.base}/uapi/domestic-stock/v1/quotations/inquire-price"
        params = {
            "fid_cond_mrkt_div_code": mkt_code,
            "fid_input_iscd": symbol
        }

        try:
            # _call_api_with_retry를 사용하여 403 에러 발생 시 자동 갱신
            resp = self._call_api_with_retry("GET", url, tr_id, params=params)
            data = resp.json()
            
            # "prdy_ctrt": 전일 대비율 (예: "15.5", "-3.2")
            rate_str = data.get("output", {}).get("prdy_ctrt")
            
            if rate_str:
                return float(rate_str)
            
            return None
            
        except Exception as e:
            primary_error = e

        try:
            snapshot = fetch_naver_quote_snapshot(symbol)
            rate = snapshot.get("change_rate")
            if rate is not None:
                if primary_error is not None:
                    log.info("[KIS][FALLBACK] get_price_change_rate switched to Naver: %s -> %s | primary=%s", symbol, rate, str(primary_error)[:120])
                return float(rate)
        except Exception as e:
            if primary_error is not None:
                log.warning(f"[KIS] get_price_change_rate failed and fallback also failed: {symbol} | primary={primary_error} | fallback={e}")
            else:
                log.warning(f"[KIS][FALLBACK] get_price_change_rate failed: {symbol} | {e}")
        if primary_error is not None:
            log.warning(f"[KIS] get_price_change_rate failed: {symbol} | {primary_error}")
        return None
