import io
import re
import time
import zipfile
import requests
from typing import Optional, List, Dict, Tuple, Literal

BASE_URL_OPENDART = "https://opendart.fss.or.kr"
ENDPOINT_CORP_CODE = "/api/corpCode.xml"
ENDPOINT_LIST = "/api/list.json"

AllowedKind = Literal["ORDER", "PERF"]

# -------------------------------------------------------------------
# 제목 정규화/필터
# -------------------------------------------------------------------

def _norm_title(s: str) -> str:
    """
    DART 제목은 공백/중간점(ㆍ·)/괄호/대괄호 등 변형이 많아서 비교 전에 강하게 정규화.
    """
    s = s or ""
    s = re.sub(r"\s+", "", s)
    s = s.replace("ㆍ", "").replace("·", "").replace("･", "").replace("・", "")
    # 괄호/대괄호/중괄호 제거
    s = re.sub(r"[\[\]\(\)\{\}]", "", s)
    return s.strip()


# ✅ 정정류: HTML 경로에서 특히 치명적이므로 "prefix 기반"으로 강하게 제외
#   예: [기재정정] 단일판매ㆍ공급계약체결 -> 정규화 후 "기재정정단일판매공급계약체결"
CORRECTION_PREFIXES = [
    "기재정정", "정정", "첨부정정", "변경", "추가정정", "정정신고",
]

def _is_correction_title(title: str) -> bool:
    t = _norm_title(title)
    return any(t.startswith(_norm_title(p)) for p in CORRECTION_PREFIXES)


def _strip_correction_prefix(title: str) -> str:
    t = _norm_title(title)
    changed = True
    while changed and t:
        changed = False
        for prefix in CORRECTION_PREFIXES:
            normalized = _norm_title(prefix)
            if normalized and t.startswith(normalized):
                t = t[len(normalized):]
                changed = True
                break
    return t


# ✅ 전체적으로 불필요한 공시류를 빠르게 제거(그래도 최종은 allowlist가 결정)
GLOBAL_SKIP_KEYWORDS = [
    "동일인", "자기주식", "의결권", "특수관계인", "주주총회", "주요사항", "타법인",
    "공정거래", "일괄신고", "최대주주", "효력발생", "증권발행", "기업설명회",
    "채무보증", "신탁계약", "주요내용", "증권신고서", "투자판단", "주식소각",
    "주식등의대량보유", "주요주주", "자회사"
]

# ✅ “실적 예고/예정” 같은 것들(실적 발표가 아니라 일정 공지라서 제외)
NEGATIVE_KEYWORDS = [
    "예고", "안내공시", "공시예정", "예정일",
    "결산실적공시예고", "결산실적공시예정일",
    "증권발행실적"
]

# -------------------------------------------------------------------
# ✅ 핵심: “수주 + 잠정실적”만 통과시키는 화이트리스트
# -------------------------------------------------------------------

def is_order_received_report(title: str) -> bool:
    """
    수주공시: 단일판매/공급계약체결
    ✅ 정정류는 무조건 제외 (HTML 경로에서도 이 함수가 바로 호출됨)
    """
    if _is_correction_title(title):
        return False

    t = _norm_title(title)
    return "단일판매공급계약체결" in t


def is_supply_contract_update_title(title: str) -> bool:
    t = _norm_title(title)
    return "단일판매공급계약" in t and any(k in t for k in ("변경", "정정", "수정"))


def is_supply_contract_termination_title(title: str) -> bool:
    t = _norm_title(title)
    return "단일판매공급계약" in t and any(k in t for k in ("해지", "취소", "철회", "종료"))


def is_perf_report_title(title: str) -> bool:
    """
    실적발표: “잠정” + “실적” 포함
    ✅ 예고/예정/안내공시는 제외
    ✅ 정정류도 제외
    """
    if _is_correction_title(title):
        return False

    t = _norm_title(title)

    if any(_norm_title(k) in t for k in NEGATIVE_KEYWORDS):
        return False

    return ("잠정" in t) and ("실적" in t)


def is_buyback_report_title(title: str) -> bool:
    t = _norm_title(title)
    if "신탁" in t:
        return False
    if "결정" not in t:
        return False
    if "처분" in t:
        return False
    return any(k in t for k in ("자사주", "자기주식", "주식소각"))


def is_dilution_report_title(title: str) -> bool:
    t = _norm_title(title)
    keywords = (
        "유상증자결정",
        "무상증자결정",
        "전환사채권발행결정",
        "신주인수권부사채권발행결정",
        "교환사채권발행결정",
        "전환청구권행사",
        "신주인수권행사",
    )
    return any(k in t for k in keywords)


def is_large_holder_report_title(title: str) -> bool:
    t = _norm_title(title)
    return any(
        k in t for k in (
            "주식등의대량보유상황보고서",
            "대량보유상황보고서",
            "대량보유보고서",
        )
    )


def is_insider_trade_report_title(title: str) -> bool:
    t = _norm_title(title)
    return "임원주요주주특정증권등소유상황보고서" in t


def is_sales_variation_title(title: str) -> bool:
    t = _norm_title(title).replace("％", "%")
    return (
        (("매출액" in t) or ("손익구조" in t))
        and (("30%" in t) or ("15%" in t) or ("대규모법인" in t))
        and (("변동" in t) or ("변경" in t))
    )


def is_dividend_report_title(title: str) -> bool:
    t = _norm_title(title)
    keywords = (
        "현금현물배당결정",
        "주식배당결정",
        "결산배당결정",
        "분기배당결정",
        "중간배당결정",
    )
    return any(k in t for k in keywords)


def is_stock_split_title(title: str) -> bool:
    t = _norm_title(title)
    return any(k in t for k in ("주식분할결정", "액면분할"))


def is_reverse_split_or_reduction_title(title: str) -> bool:
    t = _norm_title(title)
    return any(k in t for k in ("감자결정", "주식병합결정", "액면병합"))


def is_merger_title(title: str) -> bool:
    t = _norm_title(title)
    if "분할합병" in t:
        return False
    return any(k in t for k in ("합병결정", "회사합병결정", "소규모합병"))


def is_spinoff_title(title: str) -> bool:
    t = _norm_title(title)
    return any(k in t for k in ("회사분할결정", "인적분할결정", "물적분할결정", "분할합병결정"))


def _is_supply_contract_termination_text(text: str) -> bool:
    return "단일판매공급계약" in text and any(k in text for k in ("해지", "취소", "철회", "종료"))


def _is_supply_contract_update_text(text: str) -> bool:
    return "단일판매공급계약" in text and any(k in text for k in ("변경", "정정", "수정"))


def _is_order_received_text(text: str) -> bool:
    return "단일판매공급계약체결" in text


def _is_perf_report_text(text: str) -> bool:
    if any(_norm_title(k) in text for k in NEGATIVE_KEYWORDS):
        return False
    return ("잠정" in text) and ("실적" in text)


def _is_stock_cancellation_text(text: str) -> bool:
    return any(k in text for k in ("주식소각결정", "자기주식소각결정", "자사주소각결정", "주식소각"))


def _is_buyback_disposal_text(text: str) -> bool:
    if "신탁" in text or "결정" not in text:
        return False
    return ("처분" in text) and any(k in text for k in ("자사주", "자기주식"))


def _is_buyback_acquisition_text(text: str) -> bool:
    if "신탁" in text or "결정" not in text or "처분" in text:
        return False
    return any(k in text for k in ("자사주", "자기주식"))


def _is_dividend_report_text(text: str) -> bool:
    keywords = (
        "현금현물배당결정",
        "현금배당결정",
        "주식배당결정",
        "결산배당결정",
        "분기배당결정",
        "중간배당결정",
    )
    return any(k in text for k in keywords)


def _is_stock_split_text(text: str) -> bool:
    return any(k in text for k in ("주식분할결정", "액면분할"))


def _is_reverse_split_or_reduction_text(text: str) -> bool:
    return any(k in text for k in ("감자결정", "주식병합결정", "액면병합"))


def _is_merger_text(text: str) -> bool:
    if "분할합병" in text:
        return False
    return any(k in text for k in ("합병결정", "회사합병결정", "소규모합병", "흡수합병"))


def _is_spinoff_text(text: str) -> bool:
    return any(k in text for k in ("회사분할결정", "인적분할결정", "물적분할결정", "분할합병결정"))


def _is_dilution_text(text: str) -> bool:
    keywords = (
        "유상증자결정",
        "무상증자결정",
        "전환사채권발행결정",
        "신주인수권부사채권발행결정",
        "교환사채권발행결정",
        "전환청구권행사",
        "신주인수권행사",
    )
    return any(k in text for k in keywords)


def _is_sales_variation_text(text: str) -> bool:
    text = text.replace("％", "%")
    return (
        (("매출액" in text) or ("손익구조" in text))
        and (("30%" in text) or ("15%" in text) or ("대규모법인" in text))
        and (("변동" in text) or ("변경" in text))
    )


_OTHER_DISCLOSURE_KEYWORDS = (
    "주요사항보고서",
    "투자판단관련주요경영사항",
    "조회공시요구답변",
    "조회공시요구",
    "타법인주식및출자증권취득결정",
    "타법인주식및출자증권처분결정",
    "유형자산취득결정",
    "유형자산처분결정",
    "영업정지",
    "조업중단",
    "회생절차개시신청",
    "소송등의제기신청",
    "단기차입금증가결정",
)

_OTHER_DISCLOSURE_SKIP_KEYWORDS = (
    "사업보고서",
    "반기보고서",
    "분기보고서",
    "감사보고서",
    "주주총회소집공고",
    "주식등의대량보유상황보고서",
    "임원주요주주특정증권등소유상황보고서",
)


def _is_other_disclosure_text(text: str) -> bool:
    if any(keyword in text for keyword in _OTHER_DISCLOSURE_SKIP_KEYWORDS):
        return False
    return any(keyword in text for keyword in _OTHER_DISCLOSURE_KEYWORDS)


def classify_disclosure_event(title: str) -> Optional[str]:
    if not title:
        return None

    t = _norm_title(title)
    event_text = _strip_correction_prefix(title)

    if _is_supply_contract_termination_text(event_text):
        return "SUPPLY_TERMINATION"
    if _is_reverse_split_or_reduction_text(event_text):
        return "REVERSE_SPLIT_REDUCTION"
    if _is_stock_split_text(event_text):
        return "STOCK_SPLIT"
    if _is_merger_text(event_text):
        return "MERGER"
    if _is_spinoff_text(event_text):
        return "SPINOFF"
    if _is_dividend_report_text(event_text):
        return "DIVIDEND"
    if _is_dilution_text(event_text):
        return "DILUTION"
    if _is_stock_cancellation_text(event_text):
        return "STOCK_CANCELLATION"
    if _is_buyback_disposal_text(event_text):
        return "BUYBACK_DISPOSAL"
    if _is_buyback_acquisition_text(event_text):
        return "BUYBACK"
    if _is_sales_variation_text(event_text):
        return "SALES_VARIATION"
    if _is_perf_report_text(event_text):
        return "PERF_PRELIM"
    if _is_order_received_text(event_text):
        return "SUPPLY_CONTRACT"
    if _is_supply_contract_update_text(event_text):
        return "SUPPLY_UPDATE"
    if is_insider_trade_report_title(title):
        return "INSIDER_OWNERSHIP"
    if is_large_holder_report_title(title):
        return "LARGE_HOLDER"
    if _is_other_disclosure_text(event_text):
        return "OTHER_DISCLOSURE"
    if _is_correction_title(title):
        return "CORRECTION"
    return None


def classify_report_title(title: str) -> Optional[AllowedKind]:
    """
    최종 판정: ORDER / PERF / None
    """
    if _is_correction_title(title):
        return None

    t = _norm_title(title)

    # 1) 전역 스킵(빠른 컷)
    if any(_norm_title(k) in t for k in GLOBAL_SKIP_KEYWORDS):
        return None

    # 2) 화이트리스트
    if "단일판매공급계약체결" in t:
        return "ORDER"
    if ("잠정" in t) and ("실적" in t) and not any(_norm_title(k) in t for k in NEGATIVE_KEYWORDS):
        return "PERF"

    return None


# -------------------------------------------------------------------
# corpCode.xml 캐시 로딩 (다운로드 1회 → dict 보관)
# -------------------------------------------------------------------

_CORP_CACHE: Dict[str, Tuple[Dict[str, str], Dict[str, str]]] = {}
# key: api_key -> (stock_to_corp, stock_to_name)

def load_corp_code_maps(api_key: str, timeout: int = 30) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    corpCode.xml(zip) 다운로드 후:
      - stock_code(6) -> corp_code(8)
      - stock_code(6) -> corp_name
    반환. api_key 단위로 캐시.
    """
    if api_key in _CORP_CACHE:
        return _CORP_CACHE[api_key]

    url = f"{BASE_URL_OPENDART}{ENDPOINT_CORP_CODE}"
    r = requests.get(url, params={"crtfc_key": api_key}, timeout=timeout)
    r.raise_for_status()

    z = zipfile.ZipFile(io.BytesIO(r.content))
    name = next((n for n in z.namelist() if n.lower().endswith(".xml")), None)
    if not name:
        stock_to_corp: Dict[str, str] = {}
        stock_to_name: Dict[str, str] = {}
        _CORP_CACHE[api_key] = (stock_to_corp, stock_to_name)
        return stock_to_corp, stock_to_name

    xml = z.read(name).decode("utf-8", errors="ignore")

    pat = re.compile(
        r"<list>.*?"
        r"<corp_code>(?P<corp>\d{8})</corp_code>.*?"
        r"<corp_name>(?P<name>.*?)</corp_name>.*?"
        r"<stock_code>(?P<stock>\d{6})</stock_code>.*?"
        r"</list>",
        re.S
    )

    stock_to_corp: Dict[str, str] = {}
    stock_to_name: Dict[str, str] = {}

    for m in pat.finditer(xml):
        stock = m.group("stock")
        corp = m.group("corp")
        cname = (m.group("name") or "").strip()
        if stock and corp:
            stock_to_corp[stock] = corp
            if cname:
                stock_to_name[stock] = cname

    _CORP_CACHE[api_key] = (stock_to_corp, stock_to_name)
    return stock_to_corp, stock_to_name


def get_corp_code(api_key: str, stock_code_6: str) -> Optional[str]:
    """
    기존 함수 인터페이스 유지.
    내부적으로 corpCode.xml을 매번 다운받지 않고 캐시 사용.
    """
    stock_code_6 = (stock_code_6 or "").strip().zfill(6)
    if not (len(stock_code_6) == 6 and stock_code_6.isdigit()):
        return None

    stock_to_corp, _ = load_corp_code_maps(api_key)
    return stock_to_corp.get(stock_code_6)


# -------------------------------------------------------------------
# list.json 조회 (corp_code optional 지원 + “수주/잠정실적만” 반환)
# -------------------------------------------------------------------

def search_recent_reports(
    api_key: str,
    corp_code: Optional[str],
    bgn_de: str,
    end_de: str,
    max_pages: int = 1,
    page_count: int = 100,  # ✅ 명세 최대 100
    corp_cls: Optional[str] = None,         # Y/K/N/E (복수 불가)
    last_reprt_at: str = "Y",               # Y면 최종정정만
    sort: str = "date",
    sort_mth: str = "desc",
    timeout: int = 15,
    sleep_sec: float = 0.12,
    max_retry_020: int = 3,
) -> List[Dict]:
    """
    - corp_code가 있으면 특정 회사만
    - corp_code=None이면 전체 최신 공시 조회(명세상 검색기간 3개월 제한)
    ✅ 여기서 “수주/잠정실적”만 남겨 반환
    """
    url = f"{BASE_URL_OPENDART}{ENDPOINT_LIST}"

    # page_count는 (1~100) :contentReference[oaicite:2]{index=2}
    page_count = max(1, min(100, int(page_count)))

    out: List[Dict] = []
    page_no = 1

    while page_no <= max_pages:
        params = {
            "crtfc_key": api_key,
            "bgn_de": bgn_de,
            "end_de": end_de,
            "page_no": page_no,
            "page_count": page_count,
            "sort": sort,
            "sort_mth": sort_mth,
            "last_reprt_at": last_reprt_at,
        }
        if corp_code:
            params["corp_code"] = corp_code
        if corp_cls:
            params["corp_cls"] = corp_cls

        data = None
        for attempt in range(max_retry_020 + 1):
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            data = r.json()

            status = str(data.get("status", ""))
            if status == "020" and attempt < max_retry_020:
                time.sleep(0.6 * (attempt + 1))
                continue
            break

        if data is None:
            break

        if str(data.get("status")) == "013":
            break

        if str(data.get("status")) != "000":
            raise RuntimeError(
                f"DART list API error: status={data.get('status')} msg={data.get('message')}"
            )

        for it in (data.get("list", []) or []):
            title = it.get("report_nm", "") or ""
            kind = classify_report_title(title)
            if kind is None:
                continue

            it["_kind"] = kind
            out.append(it)

        total_page = int(data.get("total_page") or page_no)
        if page_no >= total_page:
            break

        page_no += 1
        time.sleep(sleep_sec)

    return out


# -------------------------------------------------------------------
# 최신 공시(유가/코스닥)만 긁어서 watch_set에 해당하는 것만 반환
# -------------------------------------------------------------------

def fetch_latest_reports_for_watchlist(
    api_key: str,
    watch_set: set[str],
    bgn_de: str,
    end_de: str,
    max_pages: int = 1,
    page_count: int = 100,
    last_reprt_at: str = "Y",
) -> List[Dict]:
    """
    list.json을 corp_code 없이 최신순으로 조회한 뒤,
    stock_code가 watch_set에 있으면 남기는 헬퍼.
    """
    out: List[Dict] = []

    for cls in ("Y", "K"):
        items = search_recent_reports(
            api_key=api_key,
            corp_code=None,
            bgn_de=bgn_de,
            end_de=end_de,
            max_pages=max_pages,
            page_count=page_count,
            corp_cls=cls,
            last_reprt_at=last_reprt_at,
            sort="date",
            sort_mth="desc",
        )

        for it in items:
            sc = (it.get("stock_code") or "").strip()
            if not sc:
                continue
            sc = sc.zfill(6)
            if not (len(sc) == 6 and sc.isdigit()):
                continue
            if sc not in watch_set:
                continue
            out.append(it)

    out.sort(key=lambda x: (x.get("rcept_dt", ""), x.get("rcept_no", "")), reverse=True)
    return out
