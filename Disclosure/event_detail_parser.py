from __future__ import annotations

import io
import json
import logging
import re
import time
import zipfile
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

try:
    from config import SETTINGS
    from dart_order import _parse_order_html
    from dart_perf import DocumentParseError, PerformanceReport
except Exception:
    from Disclosure.config import SETTINGS
    from Disclosure.dart_order import _parse_order_html
    from Disclosure.dart_perf import DocumentParseError, PerformanceReport


log = logging.getLogger("disclosure.event_detail_parser")
ROOT_DIR = Path(__file__).resolve().parent
CACHE_DIR = ROOT_DIR / "events" / "cache" / "parsed_details"
PARSER_VERSION = "2026-04-09-v2"
DOCUMENT_URL = "https://opendart.fss.or.kr/api/document.xml"
_SESSION: requests.Session | None = None


def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                )
            }
        )
        _SESSION = session
    return _SESSION


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\xa0", " ")).strip()


def _compact(value: Any) -> str:
    return re.sub(r"\s+", "", _clean_text(value))


def _parse_number(value: Any) -> float | None:
    text = _clean_text(value)
    if not text or text in {"-", "해당사항없음", "미해당"}:
        return None
    text = text.replace(",", "").replace("원", "").replace("주", "").replace("%", "").replace("배", "").strip()
    if text.startswith("(") and text.endswith(")"):
        text = f"-{text[1:-1]}"
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def _parse_all_numbers(value: Any) -> list[float]:
    text = _clean_text(value).replace(",", "")
    out: list[float] = []
    for match in re.finditer(r"-?\d+(?:\.\d+)?", text):
        try:
            out.append(float(match.group(0)))
        except Exception:
            continue
    return out


def _format_won(value: Any) -> str:
    number = _parse_number(value)
    if number is None:
        return "-"
    return f"{number:,.0f}원"


def _format_pct(value: Any) -> str:
    number = _parse_number(value)
    if number is None:
        return "-"
    return f"{number:.2f}%"


def _make_metric(label: str, value: str) -> dict[str, str]:
    return {"label": label, "value": value}


def _cache_path(rcp_no: str) -> Path:
    return CACHE_DIR / f"{rcp_no}.json"


def _load_cache(rcp_no: str) -> dict[str, Any] | None:
    path = _cache_path(rcp_no)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("parser_version") != PARSER_VERSION:
        return None
    return payload


def _save_cache(rcp_no: str, payload: dict[str, Any]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _cache_path(rcp_no).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _download_document(rcp_no: str) -> tuple[str, str]:
    session = _get_session()
    response = session.get(
        DOCUMENT_URL,
        params={"crtfc_key": SETTINGS.OPEN_DART_API_KEY, "rcept_no": rcp_no},
        timeout=20,
    )
    response.raise_for_status()
    content = response.content
    if not content.startswith(b"PK"):
        return "", ""
    archive = zipfile.ZipFile(io.BytesIO(content))
    names = archive.namelist()
    if not names:
        return "", ""
    raw = archive.read(names[0])
    text = raw.decode("utf-8", "ignore")
    document_format = "xforms_html" if "<html" in text.lower() else "dart4_xml"
    return text, document_format


def _extract_pairs_from_xml(xml_text: str) -> tuple[str, list[dict[str, str]], str]:
    soup = BeautifulSoup(xml_text, "xml")
    title_tag = soup.find("DOCUMENT-NAME") or soup.find("TITLE")
    document_title = _clean_text(title_tag.get_text(" ", strip=True) if title_tag else "")
    pairs: list[dict[str, str]] = []
    for tr in soup.find_all("TR"):
        label = " / ".join(
            filter(
                None,
                (_clean_text(cell.get_text(" ", strip=True)) for cell in tr.find_all("TD")),
            )
        )
        value = " / ".join(
            filter(
                None,
                (_clean_text(cell.get_text(" ", strip=True)) for cell in tr.find_all(["TE", "TU"])),
            )
        )
        if label and value:
            pairs.append({"label": label, "value": value})
    text = _clean_text(soup.get_text(" ", strip=True))
    return document_title, pairs, text


def _extract_pairs_from_html(html_text: str) -> tuple[str, list[dict[str, str]], str]:
    soup = BeautifulSoup(html_text, "html.parser")
    title_tag = soup.find("title")
    document_title = _clean_text(title_tag.get_text(" ", strip=True) if title_tag else "")
    pairs: list[dict[str, str]] = []
    for tr in soup.find_all("tr"):
        cells = [_clean_text(cell.get_text(" ", strip=True)) for cell in tr.find_all("td")]
        cells = [cell for cell in cells if cell]
        if len(cells) >= 2:
            pairs.append({"label": " / ".join(cells[:-1]), "value": cells[-1]})
    text = _clean_text(soup.get_text(" ", strip=True))
    return document_title, pairs, text


def _find_pair_value(pairs: list[dict[str, str]], *keywords: str) -> str:
    compact_keywords = [_compact(keyword) for keyword in keywords if keyword]
    if not compact_keywords:
        return ""
    for pair in pairs:
        label = _compact(pair.get("label"))
        if all(keyword in label for keyword in compact_keywords):
            return _clean_text(pair.get("value"))
    return ""


def _find_long_value(pairs: list[dict[str, str]], *keywords: str) -> str:
    candidates: list[str] = []
    for pair in pairs:
        label = _compact(pair.get("label"))
        if all(_compact(keyword) in label for keyword in keywords):
            value = _clean_text(pair.get("value"))
            if len(value) > 20:
                candidates.append(value)
    return candidates[0] if candidates else ""


def _fallback_excerpt(text: str) -> str:
    clean = _clean_text(text)
    if not clean:
        return ""
    sentences = re.split(r"(?<=[.!?。])\s+", clean)
    joined = " ".join(sentences[:2]).strip()
    return joined[:320]


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = _clean_text(value)
        if text:
            return text
    return ""


def _extract_merger(parsed: dict[str, Any]) -> dict[str, Any]:
    pairs = parsed["pairs"]
    text = parsed["text"]
    method = _find_pair_value(pairs, "합병방법")
    purpose = _find_long_value(pairs, "합병목적")
    effect = _find_long_value(pairs, "합병의중요영향및효과")
    ratio_value = _find_pair_value(pairs, "합병비율")
    ratio_match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*:\s*([0-9]+(?:\.[0-9]+)?)", ratio_value)
    merger_ratio = f"{ratio_match.group(1)} : {ratio_match.group(2)}" if ratio_match else ratio_value
    counterparty = _find_pair_value(pairs, "합병상대회사", "회사명")
    if not counterparty:
        counterparty_match = re.search(r"존속회사\s*[:：]\s*([^-\n]+).*?소멸회사\s*[:：]\s*([^※\n]+)", method)
        if counterparty_match:
            counterparty = f"{_clean_text(counterparty_match.group(1))} / {_clean_text(counterparty_match.group(2))}"
    new_shares = _parse_number(_find_pair_value(pairs, "합병신주의종류와수", "보통주식"))
    merger_prices = re.findall(r"합병가액[:：]?\s*([0-9,]+)원", text)
    merger_price = None
    merger_price_detail = ""
    if merger_prices:
        merger_price = _parse_number(merger_prices[-1])
        if len(merger_prices) >= 2:
            merger_price_detail = f"존속 {merger_prices[0]}원 / 소멸 {merger_prices[1]}원"
        else:
            merger_price_detail = f"{merger_prices[0]}원"
    excerpt = _first_nonempty(effect, purpose, method, _fallback_excerpt(text))
    metrics = []
    if merger_ratio:
        metrics.append(_make_metric("교환비율", merger_ratio))
    if merger_price_detail:
        metrics.append(_make_metric("합병가액", merger_price_detail))
    if new_shares:
        metrics.append(_make_metric("합병신주", f"{new_shares:,.0f}주"))
    if counterparty:
        metrics.append(_make_metric("합병 상대", counterparty))
    summary_bits = []
    if merger_ratio:
        summary_bits.append(f"교환비율 {merger_ratio}")
    if merger_price_detail:
        summary_bits.append(f"합병가액 {merger_price_detail}")
    if new_shares:
        summary_bits.append(f"합병신주 {new_shares:,.0f}주")
    summary = " · ".join(summary_bits) if summary_bits else _fallback_excerpt(excerpt)
    return {
        "event_detail_summary": summary,
        "event_source_excerpt": excerpt,
        "event_key_metrics": metrics,
        "parsed_event_details": {
            "merger_method": method,
            "merger_purpose": purpose,
            "merger_effect_note": effect,
            "merger_ratio": merger_ratio,
            "merger_price": merger_price,
            "merger_price_detail": merger_price_detail,
            "merger_counterparty": counterparty,
            "merger_new_shares": new_shares,
        },
    }


def _extract_dividend(parsed: dict[str, Any]) -> dict[str, Any]:
    pairs = parsed["pairs"]
    text = parsed["text"]
    dividend_type = _find_pair_value(pairs, "배당구분")
    dividend_kind = _find_pair_value(pairs, "배당종류")
    per_share = _parse_number(_find_pair_value(pairs, "1주당배당금", "보통주식"))
    dividend_yield = _parse_number(_find_pair_value(pairs, "시가배당률", "보통주식"))
    total_amount = _parse_number(_find_pair_value(pairs, "배당금총액"))
    record_date = _find_pair_value(pairs, "배당기준일")
    payment_date = _find_pair_value(pairs, "배당금지급예정일자")
    is_special = "특별" in dividend_type or "특별" in text
    metrics = []
    if per_share:
        metrics.append(_make_metric("주당배당", f"{per_share:,.0f}원"))
    if dividend_yield:
        metrics.append(_make_metric("시가배당률", f"{dividend_yield:.2f}%"))
    if total_amount:
        metrics.append(_make_metric("배당총액", f"{total_amount / 100_000_000:.1f}억원"))
    if record_date:
        metrics.append(_make_metric("기준일", record_date))
    summary_bits = []
    if per_share:
        summary_bits.append(f"주당 {per_share:,.0f}원")
    if dividend_yield:
        summary_bits.append(f"시가배당률 {dividend_yield:.2f}%")
    if dividend_type:
        summary_bits.append(dividend_type)
    if is_special:
        summary_bits.append("특별배당")
    summary = " · ".join(summary_bits) if summary_bits else _fallback_excerpt(text)
    excerpt = _first_nonempty(
        f"{dividend_type} · {dividend_kind} · 기준일 {record_date}" if dividend_type or dividend_kind or record_date else "",
        _fallback_excerpt(text),
    )
    return {
        "event_detail_summary": summary,
        "event_source_excerpt": excerpt,
        "event_key_metrics": metrics,
        "parsed_event_details": {
            "dividend_type": dividend_type,
            "dividend_kind": dividend_kind,
            "dividend_per_share": per_share,
            "dividend_yield_pct": dividend_yield,
            "dividend_total_amount": total_amount,
            "dividend_record_date": record_date,
            "dividend_payment_date": payment_date,
            "is_special_dividend": is_special,
        },
    }


def _extract_split_or_reduction(parsed: dict[str, Any], event_type: str) -> dict[str, Any]:
    pairs = parsed["pairs"]
    text = parsed["text"]
    ratio_label = "감자비율" if event_type == "REVERSE_SPLIT_REDUCTION" else "주식분할비율"
    ratio = _parse_number(_find_pair_value(pairs, ratio_label, "보통주식"))
    reason = _find_pair_value(pairs, "감자사유") if event_type == "REVERSE_SPLIT_REDUCTION" else _find_pair_value(pairs, "분할목적")
    method = _find_pair_value(pairs, "감자방법") if event_type == "REVERSE_SPLIT_REDUCTION" else _find_pair_value(pairs, "분할방법")
    share_value = _find_pair_value(pairs, "보통주식(주)")
    share_numbers = _parse_all_numbers(share_value)
    share_before = share_numbers[0] if len(share_numbers) >= 2 else None
    share_after = share_numbers[1] if len(share_numbers) >= 2 else None
    metrics = []
    if ratio is not None:
        metrics.append(_make_metric("비율", f"{ratio:.2f}%"))
    if share_before and share_after:
        metrics.append(_make_metric("주식수", f"{share_before:,.0f}주 → {share_after:,.0f}주"))
    if method:
        metrics.append(_make_metric("방법", method))
    if reason:
        metrics.append(_make_metric("사유", reason))
    summary_bits = []
    if ratio is not None:
        summary_bits.append(f"{'감자' if event_type == 'REVERSE_SPLIT_REDUCTION' else '분할'} 비율 {ratio:.2f}%")
    if share_before and share_after:
        summary_bits.append(f"보통주 {share_before:,.0f}주 → {share_after:,.0f}주")
    summary = " · ".join(summary_bits) if summary_bits else _fallback_excerpt(text)
    excerpt = _first_nonempty(reason, method, _fallback_excerpt(text))
    return {
        "event_detail_summary": summary,
        "event_source_excerpt": excerpt,
        "event_key_metrics": metrics,
        "parsed_event_details": {
            "share_count_before": share_before,
            "share_count_after": share_after,
            "float_shares_delta_pct": ((share_after - share_before) / share_before * 100.0) if share_before and share_after else None,
            "reduction_or_split_ratio_pct": ratio,
            "capital_change_method": method,
            "capital_change_reason": reason,
        },
    }


def _extract_buyback(parsed: dict[str, Any], event_type: str) -> dict[str, Any]:
    pairs = parsed["pairs"]
    text = parsed["text"]
    shares = _parse_number(
        _first_nonempty(
            _find_pair_value(pairs, "취득예정주식", "보통주식"),
            _find_pair_value(pairs, "처분예정주식", "보통주식"),
            _find_pair_value(pairs, "소각예정주식", "보통주식"),
        )
    )
    amount = _parse_number(
        _first_nonempty(
            _find_pair_value(pairs, "취득예정금액", "보통주식"),
            _find_pair_value(pairs, "처분예정금액", "보통주식"),
            _find_pair_value(pairs, "소각예정금액", "보통주식"),
        )
    )
    price = _parse_number(
        _first_nonempty(
            _find_pair_value(pairs, "취득대상주식가격", "보통주식"),
            _find_pair_value(pairs, "처분대상주식가격", "보통주식"),
        )
    )
    purpose = _first_nonempty(_find_pair_value(pairs, "취득목적"), _find_pair_value(pairs, "처분목적"), _find_pair_value(pairs, "소각목적"))
    counterparty = _find_pair_value(pairs, "처분상대방")
    metrics = []
    if shares:
        metrics.append(_make_metric("예정 주식수", f"{shares:,.0f}주"))
    if amount:
        metrics.append(_make_metric("예정 금액", f"{amount / 100_000_000:.1f}억원"))
    if price:
        metrics.append(_make_metric("주당 가격", f"{price:,.0f}원"))
    if counterparty:
        metrics.append(_make_metric("상대방", counterparty))
    summary_bits = []
    if shares:
        summary_bits.append(f"{shares:,.0f}주")
    if amount:
        summary_bits.append(f"{amount / 100_000_000:.1f}억원")
    if purpose:
        summary_bits.append(purpose)
    summary = " · ".join(summary_bits) if summary_bits else _fallback_excerpt(text)
    excerpt = _first_nonempty(purpose, counterparty, _fallback_excerpt(text))
    return {
        "event_detail_summary": summary,
        "event_source_excerpt": excerpt,
        "event_key_metrics": metrics,
        "parsed_event_details": {
            "buyback_or_disposal_shares": shares,
            "buyback_amount": amount,
            "buyback_unit_price": price,
            "buyback_counterparty": counterparty,
            "buyback_purpose": purpose,
        },
    }


def _extract_dilution(parsed: dict[str, Any]) -> dict[str, Any]:
    pairs = parsed["pairs"]
    text = parsed["text"]
    new_shares = _parse_number(_find_pair_value(pairs, "신주의종류와수", "보통주식"))
    share_before = _parse_number(_find_pair_value(pairs, "증자전발행주식총수", "보통주식"))
    issue_price = _parse_number(_find_pair_value(pairs, "신주발행가액", "보통주식"))
    method = _find_pair_value(pairs, "증자방식")
    purpose_parts = []
    for label in ("운영자금", "시설자금", "채무상환자금", "타법인증권취득자금", "기타자금"):
        amount = _parse_number(_find_pair_value(pairs, "자금조달의목적", label))
        if amount:
            purpose_parts.append(f"{label.replace('자금','')} {amount / 100_000_000:.1f}억원")
    share_after = share_before + new_shares if share_before and new_shares else None
    dilution_pct = (new_shares / share_before * 100.0) if share_before and new_shares else None
    metrics = []
    if new_shares:
        metrics.append(_make_metric("신주 수", f"{new_shares:,.0f}주"))
    if issue_price:
        metrics.append(_make_metric("발행가", f"{issue_price:,.0f}원"))
    if dilution_pct is not None:
        metrics.append(_make_metric("희석률", f"{dilution_pct:.2f}%"))
    if method:
        metrics.append(_make_metric("증자방식", method))
    summary_bits = []
    if new_shares:
        summary_bits.append(f"신주 {new_shares:,.0f}주")
    if issue_price:
        summary_bits.append(f"발행가 {issue_price:,.0f}원")
    if dilution_pct is not None:
        summary_bits.append(f"희석률 {dilution_pct:.2f}%")
    summary = " · ".join(summary_bits) if summary_bits else _fallback_excerpt(text)
    excerpt = _first_nonempty(" / ".join(purpose_parts), method, _fallback_excerpt(text))
    return {
        "event_detail_summary": summary,
        "event_source_excerpt": excerpt,
        "event_key_metrics": metrics,
        "parsed_event_details": {
            "new_shares": new_shares,
            "share_count_before": share_before,
            "share_count_after": share_after,
            "dilution_pct": dilution_pct,
            "issue_price": issue_price,
            "dilution_method": method,
            "dilution_purpose_breakdown": purpose_parts,
        },
    }


def _extract_supply_contract(parsed: dict[str, Any], document_text: str) -> dict[str, Any]:
    pairs = parsed["pairs"]
    text = parsed["text"]
    contract = _parse_order_html(document_text)
    contract_amount = contract.contract_amount_won or _parse_number(_find_pair_value(pairs, "계약금액"))
    recent_sales = contract.recent_sales_won or _parse_number(_find_pair_value(pairs, "최근매출액"))
    sales_ratio = contract.sales_ratio_pct or _parse_number(_find_pair_value(pairs, "매출액대비"))
    contract_name = _find_pair_value(pairs, "체결계약명")
    counterparty = _find_pair_value(pairs, "계약상대")
    start_date = _find_pair_value(pairs, "계약기간", "시작일")
    end_date = _find_pair_value(pairs, "계약기간", "종료일")
    metrics = []
    if contract_amount:
        metrics.append(_make_metric("계약금액", f"{contract_amount / 100_000_000:.1f}억원"))
    if sales_ratio:
        metrics.append(_make_metric("최근매출 대비", f"{sales_ratio:.2f}%"))
    if contract_name:
        metrics.append(_make_metric("계약명", contract_name))
    if counterparty:
        metrics.append(_make_metric("상대방", counterparty))
    summary_bits = []
    if contract_amount:
        summary_bits.append(f"계약금액 {contract_amount / 100_000_000:.1f}억원")
    if sales_ratio:
        summary_bits.append(f"최근매출 대비 {sales_ratio:.2f}%")
    if contract_name:
        summary_bits.append(contract_name)
    summary = " · ".join(summary_bits) if summary_bits else _fallback_excerpt(text)
    excerpt = _first_nonempty(counterparty, contract_name, _fallback_excerpt(text))
    return {
        "event_detail_summary": summary,
        "event_source_excerpt": excerpt,
        "event_key_metrics": metrics,
        "parsed_event_details": {
            "contract_name": contract_name,
            "contract_amount": contract_amount,
            "recent_sales_amount": recent_sales,
            "sales_ratio_pct": sales_ratio,
            "contract_counterparty": counterparty,
            "contract_start_date": start_date,
            "contract_end_date": end_date,
        },
    }


def _extract_perf_prelim(parsed: dict[str, Any], document_text: str) -> dict[str, Any]:
    pairs = parsed["pairs"]
    text = parsed["text"]
    try:
        report = PerformanceReport("local", document_text)
    except DocumentParseError:
        report = None
    revenue = report.revenue if report and report.revenue is not None else None
    operating_income = report.operating_income if report and report.operating_income is not None else _parse_number(_find_pair_value(pairs, "영업이익", "당해실적"))
    net_income = report.net_income if report and report.net_income is not None else _parse_number(_find_pair_value(pairs, "당기순이익", "당해실적"))
    if revenue is None:
        revenue = _parse_number(_find_pair_value(pairs, "매출액", "당해실적"))
    metrics = []
    if revenue is not None:
        metrics.append(_make_metric("매출", f"{revenue / 100_000_000:.1f}억원"))
    if operating_income is not None:
        metrics.append(_make_metric("영업이익", f"{operating_income / 100_000_000:.1f}억원"))
    if net_income is not None:
        metrics.append(_make_metric("순이익", f"{net_income / 100_000_000:.1f}억원"))
    summary_bits = []
    if revenue is not None:
        summary_bits.append(f"매출 {revenue / 100_000_000:.1f}억원")
    if operating_income is not None:
        summary_bits.append(f"영업이익 {operating_income / 100_000_000:.1f}억원")
    if net_income is not None:
        summary_bits.append(f"순이익 {net_income / 100_000_000:.1f}억원")
    summary = " · ".join(summary_bits) if summary_bits else _fallback_excerpt(text)
    return {
        "event_detail_summary": summary,
        "event_source_excerpt": summary or _fallback_excerpt(text),
        "event_key_metrics": metrics,
        "parsed_event_details": {
            "revenue": revenue,
            "operating_income": operating_income,
            "net_income": net_income,
        },
    }


def _extract_other_disclosure(parsed: dict[str, Any]) -> dict[str, Any]:
    pairs = parsed["pairs"]
    text = parsed["text"]
    useful_pairs = [
        _make_metric(pair["label"][:24], pair["value"][:72])
        for pair in pairs
        if pair.get("label") and pair.get("value") and len(pair["value"]) <= 90
    ][:4]
    excerpt = _fallback_excerpt(text)
    summary = excerpt[:140] if excerpt else (useful_pairs[0]["value"] if useful_pairs else "")
    return {
        "event_detail_summary": summary,
        "event_source_excerpt": excerpt,
        "event_key_metrics": useful_pairs,
        "parsed_event_details": {
            "source_rows": pairs[:12],
        },
    }


def _build_base_payload(
    rcp_no: str,
    event_type: str,
    title: str,
    document_title: str,
    document_format: str,
    pairs: list[dict[str, str]],
    text: str,
) -> dict[str, Any]:
    return {
        "parser_version": PARSER_VERSION,
        "rcp_no": rcp_no,
        "event_type": event_type,
        "title": title,
        "document_title": document_title,
        "document_format": document_format,
        "pair_count": len(pairs),
        "event_detail_summary": "",
        "event_source_excerpt": "",
        "event_key_metrics": [],
        "parsed_event_details": {
            "source_rows": pairs[:12],
        },
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def get_parsed_event_document(rcp_no: Any, event_type: Any, title: Any) -> dict[str, Any]:
    normalized_rcp = re.sub(r"\D", "", str(rcp_no or ""))
    normalized_event_type = _clean_text(event_type).upper()
    normalized_title = _clean_text(title)
    if not normalized_rcp:
        return {
            "parser_version": PARSER_VERSION,
            "rcp_no": "",
            "event_type": normalized_event_type,
            "title": normalized_title,
            "document_title": "",
            "document_format": "",
            "pair_count": 0,
            "event_detail_summary": normalized_title,
            "event_source_excerpt": "",
            "event_key_metrics": [],
            "parsed_event_details": {"source_rows": []},
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }

    cached = _load_cache(normalized_rcp)
    if cached:
        return cached

    try:
        document_text, document_format = _download_document(normalized_rcp)
    except Exception as exc:
        log.warning("event document download failed rcp=%s err=%s", normalized_rcp, exc)
        payload = {
            "parser_version": PARSER_VERSION,
            "rcp_no": normalized_rcp,
            "event_type": normalized_event_type,
            "title": normalized_title,
            "document_title": "",
            "document_format": "",
            "pair_count": 0,
            "event_detail_summary": normalized_title,
            "event_source_excerpt": "",
            "event_key_metrics": [],
            "parsed_event_details": {"source_rows": []},
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        _save_cache(normalized_rcp, payload)
        return payload

    if not document_text:
        payload = {
            "parser_version": PARSER_VERSION,
            "rcp_no": normalized_rcp,
            "event_type": normalized_event_type,
            "title": normalized_title,
            "document_title": "",
            "document_format": document_format,
            "pair_count": 0,
            "event_detail_summary": normalized_title,
            "event_source_excerpt": "",
            "event_key_metrics": [],
            "parsed_event_details": {"source_rows": []},
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        _save_cache(normalized_rcp, payload)
        return payload

    if document_format == "dart4_xml":
        document_title, pairs, text = _extract_pairs_from_xml(document_text)
    else:
        document_title, pairs, text = _extract_pairs_from_html(document_text)

    payload = _build_base_payload(
        normalized_rcp,
        normalized_event_type,
        normalized_title,
        document_title,
        document_format,
        pairs,
        text,
    )
    parsed = {"pairs": pairs, "text": text}

    if normalized_event_type == "MERGER":
        payload.update(_extract_merger(parsed))
    elif normalized_event_type == "DIVIDEND":
        payload.update(_extract_dividend(parsed))
    elif normalized_event_type in {"STOCK_SPLIT", "REVERSE_SPLIT_REDUCTION"}:
        payload.update(_extract_split_or_reduction(parsed, normalized_event_type))
    elif normalized_event_type in {"BUYBACK", "BUYBACK_DISPOSAL", "STOCK_CANCELLATION"}:
        payload.update(_extract_buyback(parsed, normalized_event_type))
    elif normalized_event_type == "DILUTION":
        payload.update(_extract_dilution(parsed))
    elif normalized_event_type in {"SUPPLY_CONTRACT", "SUPPLY_UPDATE", "SUPPLY_TERMINATION"}:
        payload.update(_extract_supply_contract(parsed, document_text))
    elif normalized_event_type == "PERF_PRELIM":
        payload.update(_extract_perf_prelim(parsed, document_text))
    else:
        payload.update(_extract_other_disclosure(parsed))

    if not payload.get("event_detail_summary"):
        payload["event_detail_summary"] = _fallback_excerpt(text) or normalized_title
    if not payload.get("event_source_excerpt"):
        payload["event_source_excerpt"] = _fallback_excerpt(text)

    _save_cache(normalized_rcp, payload)
    return payload
