import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

import schedule

# 1. 경로 설정
current_dir = os.path.dirname(os.path.abspath(__file__)) 
disclosure_dir = os.path.dirname(current_dir)             
trading_root = os.path.dirname(disclosure_dir)           

if trading_root not in sys.path: sys.path.insert(0, trading_root)
if disclosure_dir not in sys.path: sys.path.insert(0, disclosure_dir)

# 2. 공통 모듈 임포트
from broker_kis import KISBroker
from utils.slack import send_slack
from config import SETTINGS
from investor_flow_provider import fetch_recent_investor_days
from signals.wics_universe import build_effective_wics_universe, save_effective_wics_universe, normalize_sector_name
try:
    from fair_value_engine import load_latest_fair_value_frame
except Exception:
    from Disclosure.fair_value_engine import load_latest_fair_value_frame
from token_manager import get_valid_tokens

APP_KEY = SETTINGS.KIS_APPKEY
APP_SECRET = SETTINGS.KIS_APPSECRET
log = logging.getLogger("signals.wics_monitor")

# WICS 26 섹터 기본 관찰 종목 12선. 대장주에만 치우치지 않도록
# 중형 대표주와 후발 관찰주를 같이 둡니다.
WICS_26_SECTORS = {
    "1. IT하드웨어 (반도체)": {"005930": "삼성전자", "000660": "SK하이닉스", "042700": "한미반도체", "403870": "HPSP", "252810": "이수페타시스", "058470": "리노공업", "074600": "원익IPS", "067310": "하나마이크론", "036930": "주성엔지니어링", "039030": "이오테크닉스", "000990": "DB하이텍", "357780": "솔브레인"},
    "2. IT소프트웨어 (플랫폼/SI)": {"035420": "NAVER", "035720": "카카오", "018260": "삼성에스디에스", "307950": "현대오토에버", "022100": "포스코DX", "012510": "더존비즈온", "053800": "안랩", "030520": "한글과컴퓨터", "058970": "엠로", "047560": "이스트소프트", "181710": "NHN", "067160": "SOOP"},
    "3. 디스플레이/IT부품": {"034220": "LG디스플레이", "009150": "삼성전기", "011070": "LG이노텍", "090460": "비에이치", "222800": "심텍", "195870": "해성디에스", "056190": "에스에프에이", "222080": "덕산네오룩스", "272290": "이녹스첨단소재", "140860": "파크시스템스", "248070": "솔루엠", "007810": "코리아써키트"},
    "4. 2차전지/배터리": {"373220": "LG에너지솔루션", "006400": "삼성SDI", "247540": "에코프로비엠", "086520": "에코프로", "003670": "포스코퓨처엠", "066970": "엘앤에프", "348370": "엔켐", "001570": "금양", "078600": "대주전자재료", "121600": "나노신소재", "450080": "에코프로머티", "005070": "코스모신소재"},
    "5. 화학/석유화학": {"051910": "LG화학", "010950": "S-Oil", "011780": "금호석유화학", "011170": "롯데케미칼", "009830": "한화솔루션", "006650": "대한유화", "298050": "HS효성첨단소재", "011790": "SKC", "120110": "코오롱인더", "004430": "송원산업", "298020": "효성티앤씨", "004000": "롯데정밀화학"},
    "6. 제약/바이오 (대형)": {"207940": "삼성바이오로직스", "068270": "셀트리온", "000100": "유한양행", "128940": "한미약품", "326030": "SK바이오팜", "302440": "SK바이오사이언스", "006280": "녹십자", "185750": "종근당", "069620": "대웅제약", "196170": "알테오젠", "195940": "HK이노엔", "003850": "보령"},
    "7. 헬스케어/의료기기": {"214150": "클래시스", "145020": "휴젤", "214450": "파마리서치", "287410": "제이시스메디칼", "338220": "뷰노", "328130": "루닛", "041830": "인바디", "206640": "바디텍메드", "145720": "덴티움", "039840": "디오", "336570": "원텍", "228670": "레이"},
    "8. 자동차/완성차": {"005380": "현대차", "000270": "기아", "003620": "KG모빌리티", "005385": "현대차우", "005387": "현대차2우B", "005389": "현대차3우B", "086280": "현대글로비스", "064960": "SNT모티브", "308170": "씨티알모빌리티", "123040": "엠에스오토텍", "224110": "에이텍모빌리티", "064350": "현대로템"},
    "9. 자동차부품/타이어": {"012330": "현대모비스", "204320": "HL만도", "018880": "한온시스템", "011210": "현대위아", "161390": "한국타이어앤테크놀로지", "073240": "금호타이어", "002350": "넥센타이어", "005850": "에스엘", "043370": "피에이치에이", "118990": "모트렉스", "010690": "화신", "200880": "서연이화"},
    "10. 조선/해양": {"009540": "HD한국조선해양", "010140": "삼성중공업", "042660": "한화오션", "329180": "HD현대중공업", "010620": "HD현대미포", "082740": "한화엔진", "075580": "세진중공업", "014620": "성광벤드", "023160": "태광", "333430": "일승", "017960": "한국카본", "033500": "동성화인텍"},
    "11. 방위산업/우주항공": {"012450": "한화에어로스페이스", "047810": "한국항공우주", "079550": "LIG넥스원", "272210": "한화시스템", "099320": "쎄트렉아이", "189300": "인텔리안테크", "361390": "제노코", "010820": "퍼스텍", "065450": "빅텍", "005870": "휴니드", "003570": "SNT다이내믹스", "448710": "코츠테크놀로지"},
    "12. 기계/공작기계": {"034020": "두산에너빌리티", "241560": "두산밥캣", "042670": "HD현대인프라코어", "267270": "HD현대건설기계", "010120": "LS ELECTRIC", "298040": "효성중공업", "267260": "HD현대일렉트릭", "105840": "우진", "017550": "수산세보틱스", "000490": "대동", "454910": "두산로보틱스", "009160": "SIMPAC"},
    "13. 철강/비철금속": {"005490": "POSCO홀딩스", "004020": "현대제철", "010130": "고려아연", "103140": "풍산", "460860": "동국제강", "001430": "세아베스틸지주", "306200": "세아제강", "084010": "대한제강", "000670": "영풍", "104700": "한국철강", "003030": "세아제강지주", "002710": "TCC스틸"},
    "14. 건설/건자재": {"000720": "현대건설", "028260": "삼성물산", "006360": "GS건설", "047040": "대우건설", "375500": "DL이앤씨", "294870": "HDC현대산업개발", "002380": "KCC", "300720": "한일시멘트", "183190": "아세아시멘트", "010780": "아이에스동서", "344820": "KCC글라스", "108670": "LX하우시스"},
    "15. 운송/해운/항공": {"011200": "HMM", "003490": "대한항공", "028670": "팬오션", "086280": "현대글로비스", "000120": "CJ대한통운", "020560": "아시아나항공", "089590": "제주항공", "272450": "진에어", "091810": "티웨이항공", "044450": "KSS해운", "002320": "한진", "298690": "에어부산"},
    "16. 전력/유틸리티": {"015760": "한국전력", "036460": "한국가스공사", "071320": "지역난방공사", "051600": "한전KPS", "052690": "한전기술", "004690": "삼천리", "017390": "서울가스", "117580": "대성에너지", "034590": "인천도시가스", "267290": "경동도시가스", "018670": "SK가스", "017940": "E1"},
    "17. 통신서비스": {"017670": "SK텔레콤", "030200": "KT", "032640": "LG유플러스", "053210": "스카이라이프", "037560": "LG헬로비전", "036630": "세종텔레콤", "039290": "인포뱅크", "094480": "갤럭시아머니트리", "064260": "다날", "046440": "KG모빌리언스", "060250": "NHN KCP", "065560": "아이즈비전"},
    "18. 통신장비/네트워크": {"050890": "쏠리드", "039560": "다산네트웍스", "032500": "케이엠더블유", "138080": "오이솔루션", "073490": "이노와이어리스", "230240": "에치에프알", "178320": "서진시스템", "061040": "알에프텍", "100590": "머큐리", "056360": "코위버", "218410": "RFHIC", "115440": "우리넷"},
    "19. 금융지주/은행": {"105560": "KB금융", "055550": "신한지주", "086790": "하나금융지주", "316140": "우리금융지주", "024110": "기업은행", "323410": "카카오뱅크", "175330": "JB금융지주", "138930": "BNK금융지주", "139130": "iM금융지주", "006220": "제주은행", "138040": "메리츠금융지주", "071050": "한국금융지주"},
    "20. 증권": {"006800": "미래에셋증권", "005940": "NH투자증권", "071050": "한국금융지주", "016360": "삼성증권", "039490": "키움증권", "003540": "대신증권", "001720": "신영증권", "003470": "유안타증권", "001200": "유진투자증권", "001500": "현대차증권", "016610": "DB증권", "030610": "교보증권"},
    "21. 보험": {"032830": "삼성생명", "000810": "삼성화재", "005830": "DB손해보험", "001450": "현대해상", "088350": "한화생명", "003690": "코리안리", "000370": "한화손해보험", "000400": "롯데손해보험", "082640": "동양생명", "000540": "흥국화재", "085620": "미래에셋생명", "138040": "메리츠금융지주"},
    "22. 엔터테인먼트/게임": {"352820": "하이브", "035900": "JYP Ent.", "041510": "에스엠", "122870": "와이지엔터테인먼트", "259960": "크래프톤", "036570": "엔씨소프트", "251270": "넷마블", "263750": "펄어비스", "293490": "카카오게임즈", "112040": "위메이드", "194480": "데브시스터즈", "078340": "컴투스"},
    "23. 화장품/뷰티": {"090430": "아모레퍼시픽", "051900": "LG생활건강", "192820": "코스맥스", "161890": "한국콜마", "237880": "클리오", "018290": "브이티", "257720": "실리콘투", "018250": "애경산업", "214420": "토니모리", "950140": "잉글우드랩", "278470": "에이피알", "352480": "씨앤씨인터내셔널"},
    "24. 의류/섬유": {"383220": "F&F", "111770": "영원무역", "031430": "신세계인터내셔날", "020000": "한섬", "105630": "한세실업", "036620": "감성코퍼레이션", "298540": "더네이쳐홀딩스", "016450": "한세예스24홀딩스", "145170": "노브랜드", "035150": "백산", "093050": "LF", "009970": "영원무역홀딩스"},
    "25. 유통/백화점": {"023530": "롯데쇼핑", "004170": "신세계", "069960": "현대백화점", "139480": "이마트", "282330": "BGF리테일", "007070": "GS리테일", "008770": "호텔신라", "037710": "광주신세계", "057050": "현대홈쇼핑", "071840": "롯데하이마트", "009240": "한샘", "453340": "현대그린푸드"},
    "26. 음식료/담배": {"097950": "CJ제일제당", "003230": "삼양식품", "004370": "농심", "271560": "오리온", "033780": "KT&G", "280360": "롯데웰푸드", "005180": "빙그레", "000080": "하이트진로", "001680": "대상", "049770": "동원F&B", "007310": "오뚜기", "005610": "SPC삼립"}
}


def _empty_stock_data():
    return {
        "code": "",
        "symbol": "",
        "rate": 0.0,
        "foreign": 0,
        "inst": 0,
        "retail": 0,
        "foreign_3d": 0,
        "inst_3d": 0,
        "retail_3d": 0,
        "f_3d": 0,
        "i_3d": 0,
        "r_3d": 0,
        "f_streak": 0,
        "i_streak": 0,
        "smart_money": 0,
        "score": 0,
        "accumulation_score": 0,
        "raw_accumulation_score": 0,
        "quiet_accumulation": False,
        "dominant_actor": "중립",
        "data_source": "missing",
        "fallback_used": False,
        "flow_confidence": 0.0,
        "flow_confidence_label": "없음",
        "flow_coverage_ratio": 0.0,
    }


def _safe_ratio(numerator, denominator):
    if not denominator:
        return 0.0
    return round(numerator / denominator, 4)


def _shrink_to_zero(raw_score, confidence):
    try:
        return int(round(float(raw_score or 0) * float(confidence or 0)))
    except Exception:
        return 0


def _format_rate(value):
    try:
        number = round(float(value or 0), 2)
    except Exception:
        number = 0.0
    if number > 0:
        return f"+{number:g}%"
    return f"{number:g}%"


def _dominant_actor_label(foreign, inst, retail):
    candidates = {
        "외인": abs(foreign),
        "기관": abs(inst),
        "개인": abs(retail),
    }
    winner = max(candidates, key=candidates.get)
    if candidates[winner] == 0:
        return "중립"
    return winner


def _market_phase_label():
    now = datetime.now()
    if now.hour < 9:
        return "pre_market"
    if now.hour < 15 or (now.hour == 15 and now.minute < 30):
        return "regular_session"
    if now.hour < 18:
        return "closing_session"
    return "after_hours"


def _build_stock_leaderboard(stock_data_dict, limit=3):
    ranked = sorted(
        stock_data_dict.items(),
        key=lambda item: (
            item[1].get("accumulation_score", 0),
            item[1].get("smart_money", 0),
            item[1].get("f_streak", 0) + item[1].get("i_streak", 0),
            -abs(item[1].get("rate", 0)),
        ),
        reverse=True,
    )
    leaderboard = []
    for stock_name, data in ranked[:limit]:
        leaderboard.append(
            {
                "stock_name": stock_name,
                "accumulation_score": data.get("accumulation_score", 0),
                "smart_money": data.get("smart_money", 0),
                "foreign": data.get("foreign", data.get("f_3d", 0)),
                "inst": data.get("inst", data.get("i_3d", 0)),
                "retail": data.get("retail", data.get("r_3d", 0)),
                "rate": data.get("rate", 0),
                "f_streak": data.get("f_streak", 0),
                "i_streak": data.get("i_streak", 0),
                "quiet_accumulation": data.get("quiet_accumulation", False),
            }
        )
    return leaderboard


def _build_sector_features(sec_name, sector_score, sec_f_3d, sec_i_3d, sec_r_3d, stock_data_dict):
    smart_money_net = sec_f_3d + sec_i_3d
    positive_smart_money_count = sum(1 for data in stock_data_dict.values() if data.get("smart_money", 0) > 0)
    quiet_accumulation_count = sum(1 for data in stock_data_dict.values() if data.get("quiet_accumulation"))
    double_streak_count = sum(
        1 for data in stock_data_dict.values() if data.get("f_streak", 0) >= 2 and data.get("i_streak", 0) >= 2
    )
    fallback_count = sum(1 for data in stock_data_dict.values() if data.get("fallback_used"))
    avg_flow_confidence = round(
        sum(float(data.get("flow_confidence", 0.0) or 0.0) for data in stock_data_dict.values()) / max(1, len(stock_data_dict)),
        3,
    )
    leaderboard = _build_stock_leaderboard(stock_data_dict)
    leader_score = leaderboard[0]["accumulation_score"] if leaderboard else 0
    leader_concentration = _safe_ratio(leader_score, sector_score) if sector_score > 0 else 0.0

    return {
        "score": sector_score,
        "smart_money_net": smart_money_net,
        "positive_smart_money_count": positive_smart_money_count,
        "positive_smart_money_ratio": _safe_ratio(positive_smart_money_count, len(stock_data_dict)),
        "quiet_accumulation_count": quiet_accumulation_count,
        "double_streak_count": double_streak_count,
        "fallback_count": fallback_count,
        "avg_flow_confidence": avg_flow_confidence,
        "dominant_actor": _dominant_actor_label(sec_f_3d, sec_i_3d, sec_r_3d),
        "leader_concentration": leader_concentration,
        "leaderboard": leaderboard,
        "sector_name": sec_name,
    }


def _build_market_summary(data_list):
    bullish = []
    risky = []
    for item in data_list:
        features = item.get("sector_features", {})
        flow = item.get("sector_flow", {})
        smart_money_net = features.get(
            "smart_money_net",
            flow.get("foreign", flow.get("f_3d", 0)) + flow.get("inst", flow.get("i_3d", 0)),
        )
        top_pick = None
        leaderboard = features.get("leaderboard") or []
        if leaderboard:
            top_pick = leaderboard[0]["stock_name"]

        if smart_money_net > 0 and features.get("positive_smart_money_count", 0) >= 3:
            bullish.append(
                {
                    "sector_name": item.get("sector_name"),
                    "score": item.get("score", 0),
                    "smart_money_net": smart_money_net,
                    "dominant_actor": features.get("dominant_actor", "중립"),
                    "top_pick": top_pick,
                }
            )

        if flow.get("retail", flow.get("r_3d", 0)) > 0 and smart_money_net <= 0:
            risky.append(
                {
                    "sector_name": item.get("sector_name"),
                    "score": item.get("score", 0),
                    "smart_money_net": smart_money_net,
                    "dominant_actor": features.get("dominant_actor", "중립"),
                    "top_pick": top_pick,
                }
            )

    bullish = sorted(bullish, key=lambda x: (x["score"], x["smart_money_net"]), reverse=True)[:5]
    risky = sorted(risky, key=lambda x: (x["score"], -x["smart_money_net"]))[:3]
    return {
        "market_phase": _market_phase_label(),
        "top_rotation_sectors": bullish,
        "risk_sectors": risky,
    }

def get_individual_stock_data(broker, symbol):
    """개별 종목의 '최근 3일' 누적 수급(억원), 주가 등락률(%) 및 연속 매집 여부를 완벽하게 계산합니다."""
    try:
        payload = fetch_recent_investor_days(broker, symbol, max_days=3)
        valid_days = payload.get("days") or []
        if not valid_days:
            return _empty_stock_data()

        latest_out = valid_days[0]
        rate = round(float(latest_out.get("rate") or 0.0), 2)

        f_3d, i_3d, r_3d = 0, 0, 0
        f_streak, i_streak = 0, 0
        for idx, day in enumerate(valid_days[:3]):
            f_amt = int(day.get("foreign_eok", 0))
            i_amt = int(day.get("inst_eok", 0))
            r_amt = int(day.get("retail_eok", 0))

            f_3d += f_amt
            i_3d += i_amt
            r_3d += r_amt

            if f_amt > 0 and f_streak == idx:
                f_streak += 1
            if i_amt > 0 and i_streak == idx:
                i_streak += 1

        smart_money = f_3d + i_3d
        quiet_accumulation = smart_money > 0 and r_3d < 0 and -4 <= rate <= 8
        raw_accumulation_score = (f_streak * 12) + (i_streak * 12)
        raw_accumulation_score += min(25, max(0, smart_money) // 50)
        if quiet_accumulation:
            raw_accumulation_score += 6
        if rate >= 15:
            raw_accumulation_score -= 6
        flow_confidence = float(payload.get("confidence_score", 0.0) or 0.0)
        accumulation_score = _shrink_to_zero(raw_accumulation_score, flow_confidence)

        return {
            "code": str(symbol).zfill(6),
            "symbol": str(symbol).zfill(6),
            "rate": rate,
            "foreign": f_3d,
            "inst": i_3d,
            "retail": r_3d,
            "foreign_3d": f_3d,
            "inst_3d": i_3d,
            "retail_3d": r_3d,
            "f_3d": f_3d,
            "i_3d": i_3d,
            "r_3d": r_3d,
            "f_streak": f_streak,
            "i_streak": i_streak,
            "smart_money": smart_money,
            "score": int(accumulation_score),
            "accumulation_score": int(accumulation_score),
            "raw_accumulation_score": int(raw_accumulation_score),
            "quiet_accumulation": quiet_accumulation,
            "dominant_actor": _dominant_actor_label(f_3d, i_3d, r_3d),
            "data_source": payload.get("source") or "missing",
            "fallback_used": bool(payload.get("fallback_used")),
            "flow_confidence": flow_confidence,
            "flow_confidence_label": payload.get("confidence_label") or "없음",
            "flow_coverage_ratio": float(payload.get("coverage_ratio", 0.0) or 0.0),
        }
    except Exception as e: 
        log.warning("Error on %s: %s", symbol, e)
    
    return _empty_stock_data()

def format_slack_report(results_list, universe_meta=None):
    sector_meta = ((universe_meta or {}).get("sectors") or {}) if isinstance(universe_meta, dict) else {}
    try:
        fair_value_df = load_latest_fair_value_frame()
    except Exception:
        fair_value_df = None
    fair_value_by_sector = {}
    if fair_value_df is not None and not fair_value_df.empty:
        for row in fair_value_df.to_dict("records"):
            sector = normalize_sector_name(row.get("sector"))
            base = row.get("fair_value_base")
            if not sector or base in (None, "", "None"):
                continue
            fair_value_by_sector.setdefault(sector, []).append(row)

    def _sector_tag(item):
        flow = item["sector_flow"]
        foreign = flow.get("foreign", flow.get("f_3d", 0))
        inst = flow.get("inst", flow.get("i_3d", 0))
        retail = flow.get("retail", flow.get("r_3d", 0))
        max_buyer = max(foreign, inst, retail)
        if max_buyer == retail:
            return "🩸개인 역행"
        if max_buyer == foreign:
            return "🇺🇸외인 우위"
        return "🏢기관 우위"

    def _top_sector_stock_names(item, limit=2):
        features = item.get("sector_features", {})
        leaderboard = features.get("leaderboard") or []
        if leaderboard:
            return [entry.get("stock_name") for entry in leaderboard[:limit] if entry.get("stock_name")]
        ranked = sorted(
            item.get("stock_data", {}).items(),
            key=lambda x: (
                x[1].get("accumulation_score", 0),
                x[1].get("smart_money", 0),
            ),
            reverse=True,
        )
        return [name for name, _ in ranked[:limit]]

    def _extra_watch_names(info, limit=4):
        info = info or {}
        names = []
        seen = set()
        final_symbols = {
            str((row or {}).get("symbol") or "").zfill(6)
            for row in (info.get("final_symbols") or [])
            if (row or {}).get("symbol")
        }

        def _append(rows, *, skip_final=False):
            for row in rows or []:
                symbol = str((row or {}).get("symbol") or "").zfill(6)
                name = str((row or {}).get("name") or "").strip()
                if not name or name in seen:
                    continue
                if skip_final and symbol and symbol in final_symbols:
                    continue
                seen.add(name)
                names.append(name)
                if len(names) >= limit:
                    return

        _append(info.get("dynamic_added"))
        _append(info.get("manual_added"))
        _append(info.get("core_review"), skip_final=True)
        return names[:limit]

    def _extra_watch_reason(info):
        info = info or {}
        manual_added = info.get("manual_added") or []
        dynamic_added = info.get("dynamic_added") or []
        if dynamic_added:
            return "종목 점검과 팩터가 함께 가리키고 부분표본 안정도도 남은 이름을 우선 확장해서 봅니다."
        if manual_added and not dynamic_added:
            return "최근 종목 점검·팩터 후보가 얇아 정적 바스켓 중심으로 봅니다."
        return ""

    def _sector_valuation_line(sector_name):
        rows = fair_value_by_sector.get(normalize_sector_name(sector_name), [])
        if not rows:
            return ""
        usable = []
        for row in rows:
            try:
                gap = float(row.get("fair_value_gap_pct") or 0.0)
                conf = float(row.get("fair_value_confidence_score") or 0.0)
            except Exception:
                continue
            usable.append((gap, conf, row))
        if not usable:
            return ""
        avg_gap = sum(gap for gap, _, _ in usable) / max(1, len(usable))
        usable.sort(key=lambda item: (item[0], item[1]), reverse=True)
        top_text = ", ".join(
            f"{(row.get('name') or row.get('symbol') or '-')}{gap:+.1f}%"
            for gap, _, row in usable[:2]
        )
        return f"   ▶️ *[적정가]* 평균 괴리 `{avg_gap:+.1f}%` | 상단 `{top_text}`"

    def _select_report_sectors(items, max_positive=8, max_risk=4):
        positive = items[:max_positive]
        selected_names = {item["sector_name"] for item in positive}
        risk_items = []
        for item in items:
            flow = item["sector_flow"]
            foreign = flow.get("foreign", flow.get("f_3d", 0))
            inst = flow.get("inst", flow.get("i_3d", 0))
            retail = flow.get("retail", flow.get("r_3d", 0))
            smart_money = foreign + inst
            if retail > 0 and smart_money <= 0 and item["sector_name"] not in selected_names:
                risk_items.append(item)
        return positive + risk_items[:max_risk], max(0, len(items) - len(positive + risk_items[:max_risk]))

    def _build_overview_lines(items):
        if not items:
            return []
        top_positive = items[:3]
        risk_items = []
        for item in items:
            flow = item["sector_flow"]
            foreign = flow.get("foreign", flow.get("f_3d", 0))
            inst = flow.get("inst", flow.get("i_3d", 0))
            retail = flow.get("retail", flow.get("r_3d", 0))
            if retail > 0 and (foreign + inst) <= 0:
                risk_items.append(item)
        lines = ["*한눈 요약*"]
        lines.append(
            "- 최근 3일 누적 상단은 `"
            + ", ".join(f"{item['sector_name']}({_sector_tag(item).replace(' ', '')})" for item in top_positive)
            + "` 입니다."
        )
        repeating = []
        for item in top_positive:
            repeating.extend(_top_sector_stock_names(item, limit=1))
        if repeating:
            lines.append("- 먼저 확인할 대장주는 `" + ", ".join(repeating[:3]) + "` 입니다.")
        if risk_items:
            lines.append("- 개인 역행 매수가 두드러진 경계 섹터는 `" + ", ".join(item["sector_name"] for item in risk_items[:3]) + "` 입니다.")
        if isinstance(universe_meta, dict):
            summary = universe_meta.get("summary") or {}
            if summary:
                lines.append(
                    "- 유니버스 조정: "
                    f"동적 편입 `{summary.get('dynamic_symbol_count', 0)}`개 | "
                    f"섹터 불일치 제외 `{summary.get('mismatch_symbol_count', 0)}`개 | "
                    f"평균 동적안정도 `{int(round(float(summary.get('avg_dynamic_stability', 0.0) or 0.0) * 100))}/100`"
                )
                lines.append(
                    "- 유니버스 안정도: "
                    f"안정형 `{summary.get('stable_sector_count', 0)}`개 | "
                    f"유동형 `{summary.get('adaptive_sector_count', 0)}`개 | "
                    f"재점검 `{summary.get('review_sector_count', 0)}`개 | "
                    f"평균 겹침률 `{summary.get('history_avg_overlap', 1.0)}` | "
                    f"표본 `{summary.get('history_confidence_label', '없음')}` "
                    f"({summary.get('history_day_count', 1)}일)"
                )
                turnover = summary.get("turnover") or {}
                if turnover:
                    lines.append(
                        "- 전회 대비: "
                        f"새 편입 `{turnover.get('added_symbol_count', 0)}`개 | "
                        f"제외 `{turnover.get('removed_symbol_count', 0)}`개"
                    )
                dynamic_rows = summary.get("dynamic_sectors") or []
                if dynamic_rows:
                    dynamic_text = " / ".join(
                        f"{row.get('sector')}({', '.join(row.get('names', [])[:2])})"
                        for row in dynamic_rows[:3]
                    )
                    lines.append("- 오늘 추가로 같이 본 종목은 `" + dynamic_text + "` 입니다.")
                    lines.append("- 동적 편입은 합의도뿐 아니라 부분표본 안정도까지 같이 확인해 흔들리는 후보를 보수적으로 줄입니다.")
                mismatch_rows = summary.get("mismatch_sectors") or []
                if mismatch_rows:
                    mismatch_text = " / ".join(
                        f"{row.get('sector')}({', '.join(row.get('names', [])[:2])})"
                        for row in mismatch_rows[:3]
                    )
                    lines.append("- 최근 다른 섹터로 읽혀 이번엔 뺀 종목은 `" + mismatch_text + "` 입니다.")
                added_rows = turnover.get("added_sectors") or []
                if added_rows:
                    added_text = " / ".join(
                        f"{row.get('sector')}({', '.join(row.get('names', [])[:2])})"
                        for row in added_rows[:3]
                    )
                    lines.append("- 전회 대비 새로 들어온 종목은 `" + added_text + "` 입니다.")
                removed_rows = turnover.get("removed_sectors") or []
                if removed_rows:
                    removed_text = " / ".join(
                        f"{row.get('sector')}({', '.join(row.get('names', [])[:2])})"
                        for row in removed_rows[:3]
                    )
                    lines.append("- 전회 대비 빠진 종목은 `" + removed_text + "` 입니다.")
                review_rows = summary.get("review_sectors") or []
                if review_rows:
                    review_text = " / ".join(row.get("sector") for row in review_rows[:3] if row.get("sector"))
                    lines.append("- 바스켓 재점검이 필요한 섹터는 `" + review_text + "` 입니다.")
            data_source = universe_meta.get("data_source_summary") or {}
            if data_source:
                kis_count = int(data_source.get("kis", 0) or 0)
                naver_count = int(data_source.get("naver_volume_fallback", 0) or 0)
                missing_count = int(data_source.get("missing", 0) or 0)
                if naver_count or missing_count:
                    lines.append(
                        "- 투자자 수급 소스: "
                        f"KIS 직결 `{kis_count}`종목 | "
                        f"Naver 보강 `{naver_count}`종목 | "
                        f"완전 실패 `{missing_count}`종목"
                    )
                    lines.append("- 보강 수급은 과신을 줄이기 위해 점수를 중립 쪽으로 수축해 반영합니다.")
        lines.append("- 이 리포트는 아래에 원시 수급표를 붙이되, 슬랙에서는 핵심 섹터만 먼저 보여줍니다.")
        return lines

    lines = []
    lines.extend(_build_overview_lines(results_list))
    if lines:
        lines.append("")

    selected_results, omitted_count = _select_report_sectors(results_list)
    for item in selected_results:
        flow = item['sector_flow']
        foreign = flow.get('foreign', flow.get('f_3d', 0))
        inst = flow.get('inst', flow.get('i_3d', 0))
        retail = flow.get('retail', flow.get('r_3d', 0))
        features = item.get("sector_features", {})

        tag = _sector_tag(item)

        lines.append(f"*{item['sector_name']}* | {tag} | 최근 3일")
        lines.append(f"   ▶️ *[총합]* 외인 `{foreign:,}억` | 기관 `{inst:,}억` | 개인 `{retail:,}억`")
        lines.append(
            f"   ▶️ *[판정]* score `{item.get('score', 0)}` | 스마트머니 `{features.get('smart_money_net', 0):,}억` "
            f"| breadth `{features.get('positive_smart_money_count', 0)}/10` | 조용한매집 `{features.get('quiet_accumulation_count', 0)}` "
            f"| 수급확신 `{int(round(float(features.get('avg_flow_confidence', 0.0) or 0.0) * 100))}`/100"
        )
        info = sector_meta.get(item["sector_name"]) or {}
        if info:
            status = (info.get("universe_status") or {}).get("label") or "유동형"
            overlap = (info.get("universe_status") or {}).get("history_avg_overlap")
            sample_count = (info.get("universe_status") or {}).get("history_sample_count")
            history_conf = (info.get("universe_status") or {}).get("history_confidence_label") or "없음"
            final_count = len(info.get("final_symbols") or [])
            lines.append(
                f"   ▶️ *[유니버스]* 최종 바스켓 `{final_count}` | "
                f"기본 바스켓 `{len(info.get('core_symbols') or [])}` | "
                f"동적 추가 `{len(info.get('dynamic_added') or [])}` | "
                f"섹터 불일치 제외 `{len(info.get('sector_mismatch_excluded') or [])}` | "
                f"안정도 `{status}` | 최근겹침 `{overlap}` ({sample_count}회, {history_conf})"
            )
            dynamic_added = info.get("dynamic_added") or []
            if dynamic_added:
                avg_dynamic_stability = sum(float(row.get("stability_confidence", 0.0) or 0.0) for row in dynamic_added) / max(1, len(dynamic_added))
                lines.append(f"   ▶️ *[동적안정도]* 평균 `{int(round(avg_dynamic_stability * 100))}/100` | 흔들리는 후보는 중립 쪽으로 줄여 반영")
            extra_names = _extra_watch_names(info)
            if extra_names:
                extra_line = "   ▶️ *[확장]* 추가로 같이 볼 종목: `" + ", ".join(extra_names) + "`"
                extra_reason = _extra_watch_reason(info)
                if extra_reason:
                    extra_line += f" | {extra_reason}"
                lines.append(extra_line)
        valuation_line = _sector_valuation_line(item["sector_name"])
        if valuation_line:
            lines.append(valuation_line)
        
        # 종목별 렌더링 (매집 점수가 높은 순으로 정렬)
        sorted_stocks = sorted(
            item['stock_data'].items(),
            key=lambda x: (
                x[1].get('accumulation_score', 0),
                x[1].get('smart_money', 0),
                x[1].get('f_streak', 0) + x[1].get('i_streak', 0),
            ),
            reverse=True,
        )
        
        detail_limit = min(6, len(sorted_stocks))
        for stock_name, s_data in sorted_stocks[:detail_limit]:
            rate_disp = _format_rate(s_data.get('rate'))
            
            # 🔥 핵심 타점: 강력한 매집 시그널 뱃지 부여
            badge = ""
            if s_data['f_streak'] == 3 and s_data['i_streak'] == 3: badge = "💎[쌍끌이3일]"
            elif s_data['f_streak'] >= 2: badge = f"🔥[외인{s_data['f_streak']}일]"
            elif s_data['i_streak'] >= 2: badge = f"🏢[기관{s_data['i_streak']}일]"
            elif s_data.get('quiet_accumulation'): badge = "🥷[조용한매집]"
            
            icon = "🔴" if s_data['rate'] > 0 else "🔵" if s_data['rate'] < 0 else "⚫"
            
            lines.append(f"      ↳ {icon}{stock_name} ({rate_disp}) {badge}")
            lines.append(
                f"         └ 외인 {s_data.get('foreign', s_data['f_3d']):,}억 | 기관 {s_data.get('inst', s_data['i_3d']):,}억 "
                f"| 개인 {s_data.get('retail', s_data['r_3d']):,}억 | score {s_data.get('accumulation_score', 0)}"
            )
        extra_stocks = [name for name, _ in sorted_stocks[detail_limit:]]
        if extra_stocks:
            lines.append("      ↳ 그외 관찰 종목: `" + ", ".join(extra_stocks[:6]) + "`")
        
        lines.append("─" * 45) 

    if omitted_count > 0:
        lines.append(f"- 생략된 섹터 `{omitted_count}`개는 JSON 로그 파일에 그대로 저장되어 있습니다.")
    return "\n".join(lines)

def save_wics_log(data_list, universe_meta=None):
    """실시간 수급 데이터를 일자별 JSON 파일로 누적 저장합니다."""
    log_dir = os.path.join(current_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    today_str = datetime.now().strftime('%Y%m%d')
    log_file = os.path.join(log_dir, f"wics_log_{today_str}.json")
    
    log_entry = {
        "captured_at": datetime.now().isoformat(timespec="seconds"),
        "timestamp": datetime.now().strftime('%H:%M:%S'),
        "market_phase": _market_phase_label(),
        "universe_meta": universe_meta or {},
        "universe_summary": (universe_meta or {}).get("summary", {}),
        "summary": _build_market_summary(data_list),
        "data": data_list
    }
    
    existing_logs = []
    if os.path.exists(log_file):
        with open(log_file, "r", encoding="utf-8") as f:
            try:
                existing_logs = json.load(f)
            except: pass
            
    existing_logs.append(log_entry)
    
    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(existing_logs, f, ensure_ascii=False, indent=2)

def run_scanner(broker, *, print_only=False, save_only=False):
    print(f"\n🚀 [WICS 26 동적 유니버스] 3일 누적 딥스캐너 가동 시작... ({datetime.now().strftime('%H:%M:%S')})")
    
    final_results = []
    effective_sectors, universe_meta = build_effective_wics_universe(WICS_26_SECTORS)
    universe_meta = save_effective_wics_universe(universe_meta)
    data_source_counts = {"kis": 0, "naver_volume_fallback": 0, "missing": 0}
    
    for sec_name, stocks in effective_sectors.items():
        sec_f_3d, sec_i_3d, sec_r_3d = 0, 0, 0
        stock_data_dict = {}
        sector_accumulation_score = 0
        
        for symbol, stock_name in stocks.items():
            data = get_individual_stock_data(broker, symbol)
            source_name = str(data.get("data_source") or "missing")
            data_source_counts[source_name] = data_source_counts.get(source_name, 0) + 1
            
            sec_f_3d += data['f_3d']
            sec_i_3d += data['i_3d']
            sec_r_3d += data['r_3d']
            
            sector_accumulation_score += data.get('accumulation_score', 0)
            
            stock_data_dict[stock_name] = data
            time.sleep(0.4) 

        sector_features = _build_sector_features(
            sec_name,
            sector_accumulation_score,
            sec_f_3d,
            sec_i_3d,
            sec_r_3d,
            stock_data_dict,
        )
            
        final_results.append({
            "sector_name": sec_name,
            "sector_flow": {
                "foreign": sec_f_3d,
                "inst": sec_i_3d,
                "retail": sec_r_3d,
                "f_3d": sec_f_3d,
                "i_3d": sec_i_3d,
                "r_3d": sec_r_3d,
            },
            "stock_data": stock_data_dict,
            "sector_features": sector_features,
            "score": sector_accumulation_score
        })
        print(f"✅ {sec_name} 수집 완료")
            
    if final_results:
        final_results.sort(key=lambda x: x['score'], reverse=True)
        if isinstance(universe_meta, dict):
            universe_meta = dict(universe_meta)
            universe_meta["data_source_summary"] = data_source_counts
        
        save_wics_log(final_results, universe_meta=universe_meta)
        report_msg = format_slack_report(final_results, universe_meta=universe_meta)
        
        now_hour = datetime.now().hour
        time_tag = "종가 확정" if now_hour >= 15 else "시초가 대비 전일" if now_hour < 9 else "장중 실시간"
        title = f"💎 WICS 26 [3일 누적 매집] 딥스캔 ({time_tag})"

        if print_only:
            print(title)
            print(report_msg)
            print("✅ print-only 모드로 리포트를 출력했습니다.")
        elif save_only:
            print("✅ save-only 모드로 유니버스와 로그만 갱신했습니다.")
        else:
            send_slack(report_msg, title=title, msg_type="success")
            print("✅ 슬랙 발송 완벽하게 완료되었습니다!")


def scheduled_job(*, print_only=False, save_only=False):
    """스케줄러가 정해진 시간에 호출할 메인 작업 함수"""
    print(f"\n⏰ 예약된 스캐닝 작업을 시작합니다. ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    try:
        rest_token, ws_key = get_valid_tokens(APP_KEY, APP_SECRET, is_virtual=False)
        broker = KISBroker(
            appkey=APP_KEY,
            appsecret=APP_SECRET,
            cano=SETTINGS.KIS_CANO,
            acnt_prdt_cd=SETTINGS.KIS_ACNT_PRDT_CD,
        )
        broker._token = rest_token

        run_scanner(broker, print_only=print_only, save_only=save_only)
    except Exception as e:
        print(f"❌ 스캐너 실행 중 에러 발생: {e}")


def _build_cli_parser():
    parser = argparse.ArgumentParser(description="WICS 26 sector flow monitor")
    parser.add_argument("--once", action="store_true", help="스케줄 루프 없이 1회만 실행합니다.")
    parser.add_argument("--print-only", action="store_true", help="슬랙 전송 없이 콘솔에만 출력합니다.")
    parser.add_argument("--save-only", action="store_true", help="유니버스와 로그만 저장하고 슬랙 전송은 생략합니다.")
    return parser


def _run_schedule_loop():
    print("🤖 WICS 딥스캐너 [schedule] 모드로 자동 가동 중...")
    print("   ↳ 목표 타격 시간: 08:00 (시초가), 15:30 (종가), 20:00 (야간)")
    print("   ↳ 코어 바스켓 + 동적 편입 구조로 섹터별 감시 종목을 유연하게 조정합니다.")

    schedule.every().day.at("08:00").do(scheduled_job)
    schedule.every().day.at("15:30").do(scheduled_job)
    schedule.every().day.at("20:00").do(scheduled_job)

    scheduled_job() 
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    args = _build_cli_parser().parse_args()
    if (args.print_only or args.save_only) and not args.once:
        raise SystemExit("--print-only/--save-only 는 --once 와 함께 사용하세요.")
    if args.once:
        scheduled_job(print_only=args.print_only, save_only=args.save_only)
    else:
        _run_schedule_loop()
