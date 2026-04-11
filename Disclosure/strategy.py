# strategy.py
from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Optional, Tuple
from datetime import datetime, time
from zoneinfo import ZoneInfo

from broker_base import BrokerClient

log = logging.getLogger("disclosure.strategy")

KST = ZoneInfo("Asia/Seoul")


# ======================
# Types
# ======================
@dataclass
class TradeDecision:
    action: str              # "BUY" | "SELL" | "SKIP"
    qty: int
    reason: str

    # ✅ 주문 파라미터(전략에서 함께 내려주기)
    # - 프리/애프터: 지정가(00) + ord_unpr=가격
    # - 정규장: 시장가(01) + ord_unpr="0"
    # - 거래소: 항상 SOR (실전 기준)
    excg_id: Optional[str] = None       # "SOR" | None(모의/기본)
    ord_dvsn: Optional[str] = None      # "00"(지정가) | "01"(시장가)
    ord_unpr: Optional[str] = None      # 지정가=가격(str) | 시장가="0"


# ======================
# Strategy
# ======================
class SimpleDisclosureStrategy:
    """
    - 실적 beat 또는 수주 hit: BUY
    - 실적 miss: (보유를 알고 있다는 전제 없이) SELL은 옵션으로만. 여기선 기본 SKIP.

    ✅ 추가된 정책(요청사항):
      - 프리/애프터: 지정가(ORD_DVSN="00")
      - 정규장: 시장가(ORD_DVSN="01")
      - 거래소 선택: 항상 SOR (EXCG_ID_DVSN_CD="SOR")  ※ 모의투자면 SOR를 빼거나 KRX로 강제
    """

    def __init__(self, max_krw_per_trade: int):
        self.max_krw_per_trade = max_krw_per_trade

    # ----------------------
    # Helpers
    # ----------------------
    def _extract_price(self, px) -> Optional[float]:
        """broker.get_last_price()가 float이거나 dict일 수 있어 보험 처리"""
        if px is None:
            return None

        if isinstance(px, dict):
            pr = px.get("output", {}).get("stck_prpr")
            try:
                return float(str(pr).replace(",", "").strip()) if pr is not None else None
            except Exception:
                return None

        try:
            return float(px)
        except Exception:
            return None

    def _now_kst_time(self) -> time:
        return datetime.now(tz=KST).time()

    def _in_range(self, t: time, start: time, end: time) -> bool:
        # inclusive start, exclusive end가 실무적으로 더 자연스러워서 end는 미포함 처리
        return start <= t < end

    def _session(self, t: Optional[time] = None) -> str:
        """
        세션 구분:
          - PRE   : 08:00 ~ 08:50  (프리)
          - REG   : 09:00 ~ 15:30  (정규장)
          - AFTER : 15:40 ~ 20:00  (애프터)

        ⚠️ 15:30~15:40은 시장 전환 구간이라 보수적으로 AFTER(지정가) 취급 가능.
        """
        t = t or self._now_kst_time()

        if self._in_range(t, time(8, 0), time(8, 50)):
            return "PRE"

        if self._in_range(t, time(9, 0), time(15, 30)):
            return "REG"

        # 보수적으로 15:30~15:40도 지정가로 처리하고 싶으면 start를 15:30으로 바꾸세요.
        if self._in_range(t, time(15, 40), time(20, 0)):
            return "AFTER"

        # 보수 옵션: 15:30~15:40을 AFTER로 포함
        if self._in_range(t, time(15, 30), time(15, 40)):
            return "AFTER"

        return "CLOSED"

    def _choose_excg_id(self, broker: BrokerClient) -> Optional[str]:
        """
        실전: SOR 고정.
        모의투자: KRX만 되는 경우가 많아서 SOR를 빼는 편이 안전.
        """
        is_paper = bool(getattr(broker, "is_paper", False))
        return None if is_paper else "SOR"

    def _get_limit_price_for_pre_after(self, broker: BrokerClient, symbol: str) -> Optional[int]:
        """
        프리/애프터 지정가 단가:
          - 가능하면 best ask/호가 기반을 쓰는 게 가장 좋지만,
            브로커 인터페이스가 없을 수 있으니 last price로 fallback.
        """
        # (선택) broker가 best ask를 제공하면 우선 사용
        best_ask = None
        getter = getattr(broker, "get_best_ask", None)
        if callable(getter):
            try:
                best_ask = getter(symbol)
            except Exception:
                best_ask = None

        if best_ask:
            try:
                v = float(best_ask)
                if v > 0:
                    return int(round(v))
            except Exception:
                pass

        # fallback: last price
        px = self._extract_price(broker.get_last_price(symbol))
        if not px or px <= 0:
            return None

        # ✅ 단순 정수 원 단위 (호가단위/틱 규칙이 엄격하면 tick rounding을 추가하세요)
        return int(round(px))

    def _build_buy_order_params(self, broker: BrokerClient, symbol: str) -> Tuple[Optional[str], Optional[str], Optional[str], str]:
        """
        반환: (excg_id, ord_dvsn, ord_unpr, why)
          - excg_id: "SOR" 또는 None(모의)
          - ord_dvsn: "00"(지정가) / "01"(시장가)
          - ord_unpr: 지정가=가격(str) / 시장가="0"
        """
        sess = self._session()
        excg_id = self._choose_excg_id(broker)

        if sess == "CLOSED":
            return None, None, None, "market closed (no valid session)"

        if sess == "REG":
            # 정규장: 시장가
            return excg_id, "01", "0", "regular session -> market order"

        # PRE / AFTER: 지정가
        limit_px = self._get_limit_price_for_pre_after(broker, symbol)
        if not limit_px or limit_px <= 0:
            return excg_id, None, None, f"{sess.lower()} session but no valid limit price"
        return excg_id, "00", str(limit_px), f"{sess.lower()} session -> limit order @{limit_px}"

    # ----------------------
    # Existing logic (qty)
    # ----------------------
    def decide_buy_qty(self, broker: BrokerClient, symbol: str, budget_krw: Optional[int] = None) -> int:
        px_raw = broker.get_last_price(symbol)
        budget = int(budget_krw or self.max_krw_per_trade or 0)
        log.info("[QTY_DEBUG] symbol=%s px=%r budget=%s", symbol, px_raw, budget)

        px = self._extract_price(px_raw)
        if not px or px <= 0:
            return 0

        qty = int(budget // px)
        return max(qty, 0)

    # ----------------------
    # Decisions
    # ----------------------
    def on_perf_signal(
        self,
        broker: BrokerClient,
        symbol: str,
        beat: bool,
        miss: bool,
        reason: str,
        budget_krw: Optional[int] = None,
    ) -> TradeDecision:
        if beat:
            qty = self.decide_buy_qty(broker, symbol, budget_krw=budget_krw) or 0
            if qty <= 0:
                return TradeDecision("SKIP", 0, "no qty")

            excg_id, ord_dvsn, ord_unpr, why = self._build_buy_order_params(broker, symbol)
            if not ord_dvsn or ord_unpr is None:
                return TradeDecision("SKIP", 0, f"cannot build order params: {why}")

            return TradeDecision(
                action="BUY",
                qty=qty,
                reason=f"perf beat: {reason}",
                excg_id=excg_id,
                ord_dvsn=ord_dvsn,
                ord_unpr=ord_unpr,
            )

        if miss:
            return TradeDecision("SKIP", 0, f"perf miss (no action default): {reason}")

        return TradeDecision("SKIP", 0, "no signal")

    def on_order_hit(
        self,
        broker: BrokerClient,
        symbol: str,
        hit: bool,
        reason: str,
        budget_krw: Optional[int] = None,
    ) -> TradeDecision:
        if not hit:
            return TradeDecision("SKIP", 0, "no hit")

        qty = self.decide_buy_qty(broker, symbol, budget_krw=budget_krw) or 0
        if qty <= 0:
            return TradeDecision("SKIP", 0, "no qty")

        excg_id, ord_dvsn, ord_unpr, why = self._build_buy_order_params(broker, symbol)
        if not ord_dvsn or ord_unpr is None:
            return TradeDecision("SKIP", 0, f"cannot build order params: {why}")

        return TradeDecision(
            action="BUY",
            qty=qty,
            reason=f"order hit: {reason}",
            excg_id=excg_id,
            ord_dvsn=ord_dvsn,
            ord_unpr=ord_unpr,
        )
