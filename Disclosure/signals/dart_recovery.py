import time
import logging

log = logging.getLogger("disclosure.recovery")

class DisclosureRecoveryMonitor:
    def __init__(
        self,
        required_drop_pct=7.0,
        required_bounce_pct=3.0,
        window_minutes=120,
        required_recovery_ratio=0.5,
    ):
        """
        :param required_drop_pct: 고점 대비 최소 하락률 (예: 7.0%) -> "충분히 털어냈는가"
        :param required_bounce_pct: 저점 대비 최소 반등률 (예: 3%) -> "확실히 돈이 다시 들어왔는가"
        :param window_minutes: 감시 유지 시간 (예: 120분 = 2시간)
        """
        self.trackers = {}
        self.required_drop_pct = required_drop_pct
        self.required_bounce_pct = required_bounce_pct
        self.window_minutes = window_minutes
        self.required_recovery_ratio = required_recovery_ratio

    def start_tracking(self, symbol: str, base_price: float, **metadata):
        """
        engine.py에서 공시 분석 성공 시 호출하여 감시를 시작합니다.
        """
        self.trackers[symbol] = {
            'start_time': time.time(),
            'base_price': base_price,
            'high_price': base_price, # 갱신될 최고가
            'low_price': base_price,  # 갱신될 최저가
            'state': 'WAITING_DROP',   # 상태: WAITING_DROP -> WAITING_BOUNCE -> TRIGGERED
            'required_drop_pct': float(metadata.get('required_drop_pct', self.required_drop_pct)),
            'required_bounce_pct': float(metadata.get('required_bounce_pct', self.required_bounce_pct)),
            'required_recovery_ratio': float(metadata.get('required_recovery_ratio', self.required_recovery_ratio)),
            'metadata': metadata,
        }
        log.info(f"🎯 [RECOVERY-INIT] {symbol} 다이나믹 V자 반등 감시 시작. (기준가: {base_price:,.0f}원)")

    def register(self, symbol: str, base_price: float, **metadata):
        self.start_tracking(symbol, base_price, **metadata)

    def check_signal(self, symbol: str, current_price: float) -> bool:
        """
        5초마다 현재가를 받아와서 V자 반등 패턴이 완성되었는지 확인합니다.
        """
        if symbol not in self.trackers:
            return False

        t = self.trackers[symbol]
        elapsed_min = (time.time() - t['start_time']) / 60.0

        # 1. 시간 초과 체크 (설정된 시간이 지나면 감시 종료)
        if elapsed_min > self.window_minutes:
            log.info(f"⏰ [RECOVERY-TIMEOUT] {symbol} {self.window_minutes}분 경과로 감시 조용히 종료")
            self.trackers.pop(symbol, None)
            return False

        # 2. 고점/저점 실시간 갱신 로직
        if current_price > t['high_price']:
            t['high_price'] = current_price
            # 고점이 갱신되면, 다시 그 고점부터 하락폭을 재야 하므로 저점도 같이 끌어올립니다.
            if t['state'] == 'WAITING_DROP':
                t['low_price'] = current_price 
        
        if current_price < t['low_price']:
            t['low_price'] = current_price

        # 3. 퍼센트 계산
        drop_from_high = ((t['high_price'] - current_price) / t['high_price']) * 100.0
        bounce_from_low = ((current_price - t['low_price']) / t['low_price']) * 100.0
        drawdown_size = max(t['high_price'] - t['low_price'], 0.0)
        recovery_ratio = 0.0
        if drawdown_size > 0:
            recovery_ratio = (current_price - t['low_price']) / drawdown_size

        # --- [디버깅 로깅] 매 틱마다 상태를 알고 싶다면 주석을 푸세요 ---
        # log.info(f"🔎 [RECOVERY-TRACE] {symbol} | 현재가:{current_price:,.0f} | 고점대비: -{drop_from_high:.1f}% | 저점대비반등: +{bounce_from_low:.1f}% | 상태:{t['state']}")
        
        # 4. 상태 머신 (패턴 인식)
        if t['state'] == 'WAITING_DROP':
            # 고점 대비 설정된 퍼센트(5%) 이상 급락했는가? (세력이 물량을 털었는가)
            if drop_from_high >= t['required_drop_pct']:
                t['state'] = 'WAITING_BOUNCE'
                log.warning(f"📉 [RECOVERY-DROP] {symbol} 고점({t['high_price']:,.0f}) 대비 -{drop_from_high:.2f}% 급락 포착! 바닥 확인 및 반등 대기 중...")
        
        elif t['state'] == 'WAITING_BOUNCE':
            # 저점 대비 설정된 퍼센트(2.5%) 이상 고개를 들었는가? (바닥 다지고 매수세 유입)
            if (
                bounce_from_low >= t['required_bounce_pct']
                and recovery_ratio >= t['required_recovery_ratio']
            ):
                log.info(f"🚀 [RECOVERY-BOUNCE] {symbol} 저점({t['low_price']:,.0f}) 대비 +{bounce_from_low:.2f}% V자 반등 성공! (매수 타점)")
                self.trackers.pop(symbol, None) # 신호 발생 후 리스트에서 삭제
                return True

        return False
