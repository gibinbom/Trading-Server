import sys
import os
import requests
import xml.etree.ElementTree as ET
import asyncio
import warnings
from datetime import datetime
import urllib
import logging
from collections import Counter, defaultdict
import json
import re

# 1. 경로 설정 및 공통 모듈 임포트
current_dir = os.path.dirname(os.path.abspath(__file__))
disclosure_dir = os.path.dirname(current_dir)
trading_root = os.path.dirname(disclosure_dir)
REPORT_DIR = os.path.join(current_dir, "reports")

if trading_root not in sys.path: sys.path.insert(0, trading_root)
if disclosure_dir not in sys.path: sys.path.insert(0, disclosure_dir)

from utils.slack import send_slack
from config import SETTINGS

try:
    from google import genai as google_genai
except Exception:
    google_genai = None

try:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        import google.generativeai as google_generativeai
except Exception:
    google_generativeai = None

log = logging.getLogger("signals.macro_news_monitor")
GEMINI_API_KEY = getattr(SETTINGS, "GEMINI_API_KEY", "")

if google_generativeai is not None and GEMINI_API_KEY:
    google_generativeai.configure(api_key=GEMINI_API_KEY)


def _ai_unavailable_reason():
    if not GEMINI_API_KEY:
        return "Gemini API 키가 비어 있습니다. `Disclosure/config.py`의 `HARDCODED_GEMINI_API_KEY` 또는 PM2/env의 `GEMINI_API_KEY`를 설정하세요."
    if google_genai is None and google_generativeai is None:
        return "`google-genai` 또는 `google-generativeai` 패키지가 설치되지 않았습니다."
    return "AI 클라이언트를 초기화하지 못했습니다."


MACRO_BUCKETS = {
    "환율/금리": ["환율", "원화", "원·달러", "달러", "연준", "파월", "금리", "cpi", "인플레이션", "물가", "한은"],
    "전쟁/원자재": ["유가", "호르무즈", "이란", "이스라엘", "전쟁", "원자재", "관세", "수출", "가스"],
    "반도체/AI": ["반도체", "ai", "엔비디아", "애플", "테슬라", "메가테크"],
    "국내정책/내수": ["밸류업", "공급망", "실적발표", "면세", "내수", "소비"],
}

MACRO_WICS_PLAYBOOK = {
    "환율/금리": {
        "watch": [
            {"sector": "IT하드웨어 (반도체)", "leaders": ["삼성전자", "SK하이닉스"], "reason": "환율과 수출 민감도가 높고 시장 대표성이 큽니다."},
            {"sector": "금융지주/은행", "leaders": ["KB금융", "신한지주"], "reason": "금리 기대와 방어적 자금 이동이 함께 붙기 쉽습니다."},
            {"sector": "보험", "leaders": ["삼성생명", "삼성화재"], "reason": "변동성 장세에서 방어적 선택지로 같이 거론되기 쉽습니다."},
        ],
        "caution": [
            {"sector": "유통/백화점", "leaders": ["롯데쇼핑", "신세계"], "reason": "환율과 소비 둔화 우려를 동시에 받을 수 있습니다."},
            {"sector": "운송/해운/항공", "leaders": ["대한항공", "제주항공"], "reason": "유가와 환율이 같이 흔들리면 부담이 커질 수 있습니다."},
        ],
    },
    "전쟁/원자재": {
        "watch": [
            {"sector": "방위산업/우주항공", "leaders": ["한화에어로스페이스", "LIG넥스원"], "reason": "지정학 뉴스에 가장 직접적으로 반응하는 편입니다."},
            {"sector": "전력/유틸리티", "leaders": ["한국전력", "한국가스공사"], "reason": "에너지 가격 변동의 수혜·피해를 가장 빨리 확인할 수 있습니다."},
            {"sector": "화학/석유화학", "leaders": ["S-Oil", "LG화학"], "reason": "유가 변화가 실적 기대에 직접 연결되기 쉽습니다."},
        ],
        "caution": [
            {"sector": "운송/해운/항공", "leaders": ["대한항공", "HMM"], "reason": "원가와 운임 변동성을 함께 받을 수 있습니다."},
            {"sector": "유통/백화점", "leaders": ["신세계", "이마트"], "reason": "소비 심리 둔화 우려가 겹치면 약해질 수 있습니다."},
        ],
    },
    "반도체/AI": {
        "watch": [
            {"sector": "IT하드웨어 (반도체)", "leaders": ["삼성전자", "SK하이닉스"], "reason": "메가테크 수요와 가장 직접적으로 연결되는 핵심 축입니다."},
            {"sector": "디스플레이/IT부품", "leaders": ["LG이노텍", "비에이치"], "reason": "AI·하드웨어 투자 확산의 후행 수혜를 받기 쉽습니다."},
            {"sector": "IT소프트웨어 (플랫폼/SI)", "leaders": ["NAVER", "더존비즈온"], "reason": "AI 투자 스토리가 확장될 때 같이 묶여 움직일 수 있습니다."},
        ],
        "caution": [
            {"sector": "유통/백화점", "leaders": ["롯데쇼핑", "현대백화점"], "reason": "성장주 선호가 강할 때 상대적으로 관심에서 밀릴 수 있습니다."},
        ],
    },
    "국내정책/내수": {
        "watch": [
            {"sector": "유통/백화점", "leaders": ["롯데쇼핑", "신세계"], "reason": "내수·소비 정책 기대가 붙을 때 먼저 반응하기 쉽습니다."},
            {"sector": "금융지주/은행", "leaders": ["KB금융", "하나금융지주"], "reason": "정책·주주환원 이슈와 같이 묶여 해석되기 좋습니다."},
            {"sector": "음식료/담배", "leaders": ["CJ제일제당", "오리온"], "reason": "방어적 내수 대안으로 같이 읽히는 경우가 많습니다."},
        ],
        "caution": [
            {"sector": "IT하드웨어 (반도체)", "leaders": ["삼성전자", "SK하이닉스"], "reason": "국내 정책보다 글로벌 수요가 더 중요한 구간일 수 있습니다."},
        ],
    },
}

REQUEST_TIMEOUT_SEC = 10
MAX_HEADLINES_PER_QUERY = 6
MAX_HEADLINES_TOTAL = 16
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
}


def _clean_headline(title: str) -> str:
    text = str(title or "").strip()
    if " - " in text:
        text = text.rsplit(" - ", 1)[0].strip()
    text = re.sub(r"\s+", " ", text)
    text = text.replace("…", "...")
    return text.strip(" -")


def _headline_key(title: str) -> str:
    text = _clean_headline(title).lower()
    text = re.sub(r"[^0-9a-z가-힣]+", "", text)
    return text


def _headline_tokens(title: str) -> set[str]:
    base = _clean_headline(title).lower()
    tokens = re.findall(r"[0-9a-z가-힣]+", base)
    return {token for token in tokens if len(token) >= 2}


def _is_similar_headline(title: str, seen_titles: list[str]) -> bool:
    current_tokens = _headline_tokens(title)
    if not current_tokens:
        return False
    for existing in seen_titles:
        other_tokens = _headline_tokens(existing)
        if not other_tokens:
            continue
        overlap = len(current_tokens & other_tokens)
        if overlap >= max(3, int(min(len(current_tokens), len(other_tokens)) * 0.7)):
            return True
    return False


def _headlines_from_text(news_text: str | None) -> list[str]:
    return [line.lstrip("- ").strip() for line in str(news_text or "").splitlines() if line.strip()]


def _bucket_headlines(headlines):
    grouped = defaultdict(list)
    for headline in headlines:
        text = str(headline or "")
        lowered = text.lower()
        for bucket, keywords in MACRO_BUCKETS.items():
            if any(keyword.lower() in lowered for keyword in keywords):
                grouped[bucket].append(text)
                break
        else:
            grouped["기타"].append(text)
    return grouped


def _build_macro_take(headlines):
    grouped = _bucket_headlines(headlines)
    counts = Counter({bucket: len(items) for bucket, items in grouped.items() if items})
    ordered = [bucket for bucket, _ in counts.most_common()]
    overview = []
    watch = []
    caution = []

    if "환율/금리" in ordered:
        overview.append("환율과 금리 헤드라인이 시장의 중심에 있습니다")
        watch.extend(["반도체/수출주", "은행/보험"])
        caution.extend(["내수 소비", "고밸류 성장주"])
    if "전쟁/원자재" in ordered:
        overview.append("중동·원자재 이슈가 위험 프리미엄을 키우고 있습니다")
        watch.extend(["정유/가스", "방산", "전력/유틸리티"])
        caution.extend(["항공/운송", "원가 민감 내수"])
    if "반도체/AI" in ordered:
        overview.append("글로벌 메가테크와 반도체 체인이 계속 핵심 축입니다")
        watch.extend(["반도체", "AI 인프라"])
    if "국내정책/내수" in ordered:
        overview.append("국내 정책·실적 이슈도 개별 종목 변별력을 만들고 있습니다")
        watch.extend(["유통/백화점", "지주/주주환원"])

    dedup_watch = []
    for item in watch:
        if item not in dedup_watch:
            dedup_watch.append(item)
    dedup_caution = []
    for item in caution:
        if item not in dedup_caution:
            dedup_caution.append(item)

    dominant_bucket = ordered[0] if ordered else ""
    headline_count = sum(counts.values())
    confidence_score = min(100, 35 + headline_count * 3 + (counts.get(dominant_bucket, 0) if dominant_bucket else 0) * 5)
    if dominant_bucket in {"환율/금리", "전쟁/원자재"}:
        market_mode = "방어적"
    elif dominant_bucket == "반도체/AI":
        market_mode = "공격적"
    else:
        market_mode = "중립"

    return {
        "grouped": grouped,
        "ordered": ordered,
        "overview": overview[:3],
        "watch": dedup_watch[:4],
        "caution": dedup_caution[:4],
        "bucket_counts": dict(counts),
        "dominant_bucket": dominant_bucket,
        "headline_count": headline_count,
        "confidence_score": int(confidence_score),
        "market_mode": market_mode,
    }


def _build_watch_ideas(take: dict) -> dict:
    watch_items = []
    caution_items = []
    seen_watch = set()
    seen_caution = set()
    for bucket in take.get("ordered", []):
        playbook = MACRO_WICS_PLAYBOOK.get(bucket, {})
        for item in playbook.get("watch", []):
            sector = item.get("sector")
            if sector and sector not in seen_watch:
                watch_items.append(item)
                seen_watch.add(sector)
        for item in playbook.get("caution", []):
            sector = item.get("sector")
            if sector and sector not in seen_caution:
                caution_items.append(item)
                seen_caution.add(sector)
    return {"watch": watch_items[:4], "caution": caution_items[:3]}


def fetch_macro_headlines():
    """구글 뉴스 RSS를 통해 지난 24시간 동안의 핵심 매크로/경제 속보 headline 목록을 반환합니다."""
    print("🌍 글로벌 매크로 뉴스 수집 중...")

    queries = [
        "금리 OR 환율 OR 연준 OR 파월 OR CPI OR 인플레이션 when:1d",
        "유가 OR 지정학 OR 전쟁 OR 관세 OR 수출 OR 트럼프 when:1d",
        "반도체 OR AI OR 엔비디아 OR 애플 OR 테슬라 when:1d",
        "밸류업 OR 한은 OR 실적발표 OR 공급망 when:1d"
    ]
    all_news = []
    seen = set()
    kept_titles = []
    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)

    for q in queries:
        encoded_query = urllib.parse.quote(q)
        url = f"https://news.google.com/rss/search?q={encoded_query}&hl=ko&gl=KR&ceid=KR:ko"

        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT_SEC)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)

            for item in root.findall('.//item')[:MAX_HEADLINES_PER_QUERY]:
                title_node = item.find('title')
                clean_title = _clean_headline(title_node.text if title_node is not None else "")
                key = _headline_key(clean_title)
                if not clean_title or not key or key in seen or _is_similar_headline(clean_title, kept_titles):
                    continue
                seen.add(key)
                all_news.append(clean_title)
                kept_titles.append(clean_title)
                if len(all_news) >= MAX_HEADLINES_TOTAL:
                    break
        except Exception as e:
            print(f"❌ '{q}' 수집 실패: {e}")

        if len(all_news) >= MAX_HEADLINES_TOTAL:
            break

    return all_news


def fetch_macro_news():
    """구글 뉴스 RSS를 통해 지난 24시간 동안의 핵심 매크로/경제 속보를 긁어옵니다."""
    all_news = fetch_macro_headlines()
    if not all_news:
        return None
    return "\n".join([f"- {news}" for news in all_news])


def load_latest_macro_report() -> dict:
    latest_json = os.path.join(REPORT_DIR, "macro_news_report_latest.json")
    if not os.path.exists(latest_json):
        return {}
    try:
        with open(latest_json, "r", encoding="utf-8") as fp:
            return json.load(fp)
    except Exception:
        return {}


def _fallback_macro_report(news_text):
    headlines = _headlines_from_text(news_text)[:10]
    reason = _ai_unavailable_reason()
    take = _build_macro_take(headlines)
    ideas = _build_watch_ideas(take)
    lines = [
        "🌍 *[글로벌 매크로 & 뉴스 임팩트 분석]*",
        "",
        "*0. 시장 한 줄 요약*",
        f"- 시장 모드 `{take.get('market_mode', '중립')}` | 확신도 `{take.get('confidence_score', 0)}/100` | 중심 축 `{take.get('dominant_bucket', '-') or '-'}`",
        "",
        "*1. 관측 사실*",
    ]
    if take["overview"]:
        lines.append("- " + " / ".join(take["overview"]))
    for bucket in take["ordered"][:3]:
        samples = take["grouped"].get(bucket, [])[:2]
        if samples:
            sample_text = " / ".join(samples)
            lines.append(f"- [{bucket}] {sample_text}")
    lines.extend(
        [
            "",
            "*2. 추정*",
        ]
    )
    if take["watch"]:
        lines.append("- 한국 시장에서는 `" + ", ".join(take["watch"]) + "` 쪽을 먼저 확인하는 편이 좋습니다.")
    if take["caution"]:
        lines.append("- 반대로 `" + ", ".join(take["caution"]) + "` 쪽은 헤드라인 충격을 더 크게 받을 수 있어 보수적으로 보는 편이 낫습니다.")
    if ideas["watch"]:
        lines.append("- 오늘 먼저 볼 WICS 섹터:")
        for item in ideas["watch"][:3]:
            leaders = ", ".join(item.get("leaders", [])[:2]) or "-"
            lines.append(f"  - `{item.get('sector', '-')}` | 대표주 `{leaders}` | {item.get('reason', '')}")
    if ideas["caution"]:
        lines.append("- 보수적으로 볼 WICS 섹터:")
        for item in ideas["caution"][:2]:
            leaders = ", ".join(item.get("leaders", [])[:2]) or "-"
            lines.append(f"  - `{item.get('sector', '-')}` | 대표주 `{leaders}` | {item.get('reason', '')}")
    lines.extend(
        [
            f"- 해석상 오늘 톤은 `{take.get('market_mode', '중립')}` 에 가깝습니다.",
            "- 다만 이 해석은 헤드라인 기반 1차 분류이므로, 실제 수급/WICS 흐름과 교차 확인이 필요합니다.",
            "",
            "*3. 미확인/주의*",
            f"- {reason}",
            "- 키를 코드로 관리하려면 `Disclosure/config.py`의 `HARDCODED_GEMINI_API_KEY`에 직접 넣으면 됩니다.",
            "- 키를 PM2로 관리하려면 `ecosystem.config.js`의 `GEMINI_API_KEY`를 채운 뒤 재시작하면 됩니다.",
        ]
    )
    return "\n".join(lines)


def _save_macro_report(report_msg, news_text, source: str = "live"):
    os.makedirs(REPORT_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    headlines = _headlines_from_text(news_text)
    take = _build_macro_take(headlines)
    ideas = _build_watch_ideas(take)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": source,
        "report_text": str(report_msg or ""),
        "headlines": headlines,
        "bucket_counts": take.get("bucket_counts", {}),
        "watch_sectors": take.get("watch", []),
        "caution_sectors": take.get("caution", []),
        "watch_ideas": ideas.get("watch", []),
        "caution_ideas": ideas.get("caution", []),
        "dominant_bucket": take.get("dominant_bucket", ""),
        "market_mode": take.get("market_mode", "중립"),
        "confidence_score": int(take.get("confidence_score", 0) or 0),
        "ai_enabled": bool(GEMINI_API_KEY and (google_genai is not None or google_generativeai is not None)),
        "ai_unavailable_reason": "" if (GEMINI_API_KEY and (google_genai is not None or google_generativeai is not None)) else _ai_unavailable_reason(),
    }
    report_json = os.path.join(REPORT_DIR, f"macro_news_report_{stamp}.json")
    latest_json = os.path.join(REPORT_DIR, "macro_news_report_latest.json")
    report_txt = os.path.join(REPORT_DIR, f"macro_news_report_{stamp}.txt")
    latest_txt = os.path.join(REPORT_DIR, "macro_news_report_latest.txt")
    with open(report_json, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
    with open(latest_json, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
    with open(report_txt, "w", encoding="utf-8") as fp:
        fp.write(str(report_msg or ""))
    with open(latest_txt, "w", encoding="utf-8") as fp:
        fp.write(str(report_msg or ""))


async def _generate_ai_text(prompt):
    if google_genai is not None and GEMINI_API_KEY:
        client = google_genai.Client(api_key=GEMINI_API_KEY)
        response = await asyncio.to_thread(
            client.models.generate_content,
            model="gemini-2.5-flash",
            contents=prompt,
        )
        return getattr(response, "text", "") or ""

    if google_generativeai is not None and GEMINI_API_KEY:
        model = google_generativeai.GenerativeModel("gemini-2.5-flash")
        response = await asyncio.to_thread(model.generate_content, prompt)
        return getattr(response, "text", "") or ""

    return ""

async def analyze_news_and_predict_sectors(news_text):
    """수집된 뉴스를 Gemini에게 던져 수혜/악재 섹터를 뽑아냅니다."""
    headlines = _headlines_from_text(news_text)
    take = _build_macro_take(headlines)
    ideas = _build_watch_ideas(take)
    structured_context = [
        "[사전 구조화 요약]",
        "- 핵심 축: " + (" / ".join(take.get("overview", [])) or "-"),
        f"- 시장 모드: {take.get('market_mode', '중립')} | 확신도: {take.get('confidence_score', 0)}/100 | 중심 버킷: {take.get('dominant_bucket', '-') or '-'}",
        "- 먼저 볼 섹터: " + (", ".join(take.get("watch", [])) or "-"),
        "- 주의 섹터: " + (", ".join(take.get("caution", [])) or "-"),
        "- WICS watchlist: "
        + (", ".join(f"{item.get('sector')}({', '.join(item.get('leaders', [])[:2])})" for item in ideas.get("watch", [])) or "-"),
        "- WICS caution: "
        + (", ".join(f"{item.get('sector')}({', '.join(item.get('leaders', [])[:2])})" for item in ideas.get("caution", [])) or "-"),
        "- 버킷 카운트: "
        + (", ".join(f"{k} {v}" for k, v in take.get("bucket_counts", {}).items()) or "-"),
    ]

    prompt_instruction = """
    [임무] 당신은 여의도 최고의 '매크로 전략 퀀트 애널리스트'입니다.
    방금 수집된 지난 24시간 동안의 글로벌 매크로/경제 뉴스 헤드라인을 분석하여, 
    오늘 한국 증시(KOSPI/KOSDAQ)에서 자금이 몰릴 섹터와 빠져나갈 섹터를 예측해야 합니다.

    [분석 조건 🎯]
    1. 수혜/악재 섹터는 반드시 우리가 사용하는 'WICS 26 섹터' 범주 내에서 맵핑하세요.
       (예: 반도체, 플랫폼/AI, 2차전지, 제약/바이오, 자동차, 조선/해양, 방위산업, 철강, 전력/유틸리티, 은행, 엔터 등)
    2. 뉴스의 행간을 읽어주세요. (예: 유가 상승 -> 정유주 수혜 / 항공주 악재)
    3. 각 섹터별로 트레이더가 바로 매매에 참고할 수 있도록 '핵심 수혜 종목(대장주)' 1~2개를 반드시 짚어주세요.
    4. 출력은 반드시 `관측 사실 / 추정 / 미확인`을 구분하세요.
    5. 섹터를 길게 나열하지 말고, 오늘 한국 시장에서 먼저 확인할 3개 안팎만 남기세요.
    6. 헤드라인 복붙보다 '무슨 축이 시장을 움직이는가'를 먼저 설명하세요.

    [출력 형식]
    * 반드시 슬랙(Slack) 마크다운을 사용하여 가독성 높게 작성하세요.
    
    🌍 *[글로벌 매크로 & 뉴스 임팩트 분석]*

    *0. 시장 한 줄 요약*
    - 오늘 헤드라인을 한 문장으로 정리

    *1. 관측 사실*
    - 밤사이 핵심 헤드라인 흐름 2~4줄
    - 데이터에서 직접 읽히는 중심 축 2~3개

    *2. 추정*
    - 오늘 한국 시장에서 먼저 확인할 섹터 2~3개
    - 각 섹터마다 왜 보는지와 관심 대장주 1~2개
    - 추격보다 눌림이 나은지, 아니면 시초/장후 확인이 나은지도 한 줄로 적기

    *3. 미확인/주의*
    - 반대 시나리오 1~2개
    - 아직 확인되지 않은 변수와 주의 섹터
    """

    prompt = (
        f"{prompt_instruction}\n\n"
        + "\n".join(structured_context)
        + f"\n\n[최근 24시간 매크로 뉴스 헤드라인]\n{news_text}"
    )

    try:
        response_text = await _generate_ai_text(prompt)
        if response_text:
            return response_text
        log.warning("Gemini client unavailable; sending fallback macro digest.")
        return _fallback_macro_report(news_text)
    except Exception as e:
        log.warning("Macro AI analysis failed: %s", e)
        return _fallback_macro_report(news_text)

async def run_news_bot():
    print(f"🤖 매크로 뉴스 분석 봇 가동 시작... ({datetime.now().strftime('%H:%M:%S')})")
    
    news_text = fetch_macro_news()
    source = "live"
    
    if not news_text:
        latest = load_latest_macro_report()
        cached_headlines = latest.get("headlines") or []
        if cached_headlines:
            news_text = "\n".join(f"- {item}" for item in cached_headlines)
            source = "cached"
            print("⚠️ 라이브 수집 실패. 최신 저장본으로 대체합니다.")
        else:
            print("뉴스를 가져오지 못해 분석을 종료합니다.")
            return
        
    print("✅ 뉴스 수집 완료. AI 분석 중...")
    
    report_msg = await analyze_news_and_predict_sectors(news_text)
    _save_macro_report(report_msg, news_text, source=source)
    
    title = f"🌍 모닝 매크로 & 섹터 프리뷰 ({datetime.now().strftime('%m/%d')})"
    send_slack(report_msg, title=title, msg_type="warning")
    print("✅ 슬랙 발송 완벽하게 완료되었습니다!")

async def scheduler():
    print("⏰ 매크로 뉴스 스케줄러 가동 중 (목표 시간: 07:30)")
    while True:
        now = datetime.now()
        # 💡 매일 아침 장 시작 전, 07시 30분에 모닝 브리핑 쏘기
        if (now.hour == 7 and now.minute == 30) or (now.hour == 15 and now.minute == 30) or (now.hour == 20 and now.minute == 0):
            await run_news_bot()
            await asyncio.sleep(60) # 중복 실행 방지
            
        await asyncio.sleep(30)

if __name__ == "__main__":
    # 테스트용: 스크립트 실행 시 즉시 리포트를 보고 싶다면 아래 한 줄의 주석을 푸세요!
    # asyncio.run(run_news_bot())
    
    # 실제 스케줄러 실행
    asyncio.run(scheduler())
