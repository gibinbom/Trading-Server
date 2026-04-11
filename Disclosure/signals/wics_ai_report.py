import asyncio
import os
import sys
import json
import logging
import re
import warnings
from collections import Counter, defaultdict
from datetime import datetime, timedelta

# 상위 폴더 경로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
trading_root = os.path.dirname(parent_dir)
REPORT_DIR = os.path.join(current_dir, "reports")

if trading_root not in sys.path: sys.path.insert(0, trading_root)
if parent_dir not in sys.path: sys.path.insert(0, parent_dir)

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

# Gemini API 셋업 (Gemini 1.5 Flash 또는 2.5 Flash는 1M 토큰을 지원하므로 한 달 치 로그도 거뜬합니다!)
GEMINI_API_KEY = getattr(SETTINGS, "GEMINI_API_KEY", "")
if google_generativeai is not None and GEMINI_API_KEY:
    google_generativeai.configure(api_key=GEMINI_API_KEY)
log = logging.getLogger("signals.wics_ai_report")

WICS_THEME_RULES = {
    "성장/테크": ["IT하드웨어", "반도체", "디스플레이", "IT소프트웨어", "엔터테인먼트/게임", "플랫폼", "통신장비"],
    "금융/방어": ["금융지주/은행", "보험", "음식료/담배", "전력/유틸리티", "유통/백화점", "통신서비스"],
    "산업재/정책": ["방위산업/우주항공", "기계/공작기계", "조선/해양", "건설/건자재", "자동차/완성차", "자동차부품/타이어"],
    "소재/에너지": ["2차전지/배터리", "화학/석유화학", "철강/비철금속", "운송/해운/항공", "전기유틸리티"],
}

WICS_AGGRESSIVE_KEYWORDS = [
    "IT하드웨어", "반도체", "디스플레이", "2차전지/배터리", "엔터테인먼트/게임",
    "방위산업/우주항공", "조선/해양", "기계/공작기계", "화학/석유화학",
]
WICS_DEFENSIVE_KEYWORDS = [
    "금융지주/은행", "보험", "음식료/담배", "전력/유틸리티", "유통/백화점", "통신서비스",
]


def _ai_unavailable_reason():
    if not GEMINI_API_KEY:
        return "Gemini API 키가 비어 있습니다. `Disclosure/config.py`의 `HARDCODED_GEMINI_API_KEY` 또는 PM2/env의 `GEMINI_API_KEY`를 설정하세요."
    if google_genai is None and google_generativeai is None:
        return "`google-genai` 또는 `google-generativeai` 패키지가 설치되지 않았습니다."
    return "AI 클라이언트를 초기화하지 못했습니다."


def _clean_sector_name(name):
    text = str(name or "").strip()
    if not text:
        return ""
    return re.sub(r"^\d+\.\s*", "", text).strip()


def _flow_value(node, *keys):
    for key in keys:
        if key in node:
            return node.get(key, 0)
    return 0


def _leader_names(sector, limit=2):
    features = sector.get("sector_features", {})
    leaderboard = features.get("leaderboard") or []
    if leaderboard:
        return [item.get("stock_name") for item in leaderboard[:limit] if item.get("stock_name")]

    stock_data = sector.get("stock_data", {})
    ranked = sorted(
        stock_data.items(),
        key=lambda item: (
            item[1].get("accumulation_score", 0),
            item[1].get("smart_money", item[1].get("f_3d", 0) + item[1].get("i_3d", 0)),
        ),
        reverse=True,
    )
    return [name for name, _ in ranked[:limit]]


def _extract_universe_status_map(entry):
    universe_meta = entry.get("universe_meta") or {}
    sectors = universe_meta.get("sectors") or {}
    out = {}
    for raw_sector, info in sectors.items():
        sector_key = _clean_sector_name((info or {}).get("normalized_sector") or raw_sector)
        if not sector_key:
            continue
        out[sector_key] = {
            "label": ((info or {}).get("universe_status") or {}).get("label", ""),
            "reason": ((info or {}).get("universe_status") or {}).get("reason", ""),
        }
    return out


def _build_day_snapshot(date_str, entry):
    data = entry.get("data", [])
    summary = entry.get("summary", {})

    if summary.get("top_rotation_sectors"):
        top_rotation = summary.get("top_rotation_sectors", [])[:3]
        risk_sectors = summary.get("risk_sectors", [])[:1]
    else:
        ranked = sorted(
            data,
            key=lambda sector: (
                sector.get("score", 0),
                _flow_value(sector.get("sector_flow", {}), "foreign", "f_3d")
                + _flow_value(sector.get("sector_flow", {}), "inst", "i_3d"),
            ),
            reverse=True,
        )
        top_rotation = []
        risk_sectors = []
        for sector in ranked[:3]:
            flow = sector.get("sector_flow", {})
            smart_money_net = _flow_value(flow, "foreign", "f_3d") + _flow_value(flow, "inst", "i_3d")
            derived_score = sector.get("score", 0) or round(smart_money_net / 100, 1)
            top_rotation.append(
                {
                    "sector_name": sector.get("sector_name"),
                    "score": derived_score,
                    "smart_money_net": smart_money_net,
                    "dominant_actor": sector.get("sector_features", {}).get("dominant_actor", "중립"),
                    "top_pick": next(iter(_leader_names(sector)), None),
                }
            )
        for sector in ranked:
            flow = sector.get("sector_flow", {})
            smart_money = _flow_value(flow, "foreign", "f_3d") + _flow_value(flow, "inst", "i_3d")
            retail = _flow_value(flow, "retail", "r_3d")
            if retail > 0 and smart_money <= 0:
                risk_sectors.append(
                    {
                        "sector_name": sector.get("sector_name"),
                        "score": sector.get("score", 0) or round(abs(smart_money) / 100, 1),
                        "smart_money_net": smart_money,
                        "dominant_actor": sector.get("sector_features", {}).get("dominant_actor", "중립"),
                        "top_pick": next(iter(_leader_names(sector)), None),
                    }
                )
                break

    return {
        "date": f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}",
        "market_phase": entry.get("market_phase", "unknown"),
        "top_rotation": top_rotation,
        "risk_sectors": risk_sectors,
        "universe_summary": entry.get("universe_summary") or ((entry.get("universe_meta") or {}).get("summary") or {}),
        "universe_status_map": _extract_universe_status_map(entry),
        "raw_summary": summary,
    }


def _aggregate_sector_rows(rows, *, risk=False):
    def _leader_stability_label(share, appearances):
        share = float(share or 0)
        appearances = int(appearances or 0)
        if appearances >= 3 and share >= 0.75:
            return "고정 대장"
        if appearances >= 2 and share >= 0.5:
            return "반복 대장"
        return "순환형"

    aggregated = []
    for sector_name, stat in rows:
        appearances = stat.get("appearances", 0)
        if appearances <= 0:
            continue
        avg_score = round(stat.get("score_sum", 0) / appearances, 1)
        avg_smart = round(stat.get("smart_sum", 0) / appearances, 1)
        leader_counts = stat.get("leaders", Counter())
        top_pick = leader_counts.most_common(1)
        alt_pick = leader_counts.most_common(2)
        dominant_actor = stat.get("actors", Counter()).most_common(1)
        top_pick_name = top_pick[0][0] if top_pick else "-"
        top_pick_count = top_pick[0][1] if top_pick else 0
        leader_share = round(top_pick_count / appearances, 2) if appearances else 0.0
        alt_pick_name = alt_pick[1][0] if len(alt_pick) > 1 else ""
        aggregated.append(
            {
                "sector_name": sector_name,
                "sector_short": _clean_sector_name(sector_name),
                "appearances": appearances,
                "score": avg_score,
                "smart_money_net": avg_smart,
                "dominant_actor": dominant_actor[0][0] if dominant_actor else "-",
                "top_pick": top_pick_name,
                "top_pick_share": leader_share,
                "top_pick_count": top_pick_count,
                "alt_pick": alt_pick_name,
                "leader_regime": _leader_stability_label(leader_share, appearances),
                "risk_days": appearances if risk else 0,
            }
        )
    aggregated.sort(
        key=lambda item: (
            item.get("appearances", 0),
            item.get("score", 0),
            item.get("smart_money_net", 0),
        ),
        reverse=True,
    )
    return aggregated


def _dominant_rotation_theme(top_rotation_sectors):
    theme_counts = Counter()
    for item in top_rotation_sectors[:5]:
        sector_short = _clean_sector_name(item.get("sector_name"))
        for theme, keywords in WICS_THEME_RULES.items():
            if any(keyword in sector_short for keyword in keywords):
                theme_counts[theme] += max(1, int(item.get("appearances", 1)))
                break
    return theme_counts.most_common(1)[0][0] if theme_counts else "균형/혼조"


def _rotation_market_mode(top_rotation_sectors):
    aggressive = 0
    defensive = 0
    for item in top_rotation_sectors[:5]:
        sector_short = _clean_sector_name(item.get("sector_name"))
        appearances = max(1, int(item.get("appearances", 1)))
        if any(keyword in sector_short for keyword in WICS_AGGRESSIVE_KEYWORDS):
            aggressive += appearances
        if any(keyword in sector_short for keyword in WICS_DEFENSIVE_KEYWORDS):
            defensive += appearances
    if aggressive >= defensive + 2:
        return "공격적"
    if defensive >= aggressive + 2:
        return "방어적"
    return "중립"


def _build_rotation_ideas(top_rotation_sectors, risk_sectors):
    watch = []
    caution = []
    for item in top_rotation_sectors[:3]:
        appearances = int(item.get("appearances", 0))
        score = round(float(item.get("score", 0)), 1)
        avg_smart = round(float(item.get("smart_money_net", 0)), 1)
        leader_regime = item.get("leader_regime") or "순환형"
        universe_status = item.get("universe_status_label") or ""
        top_pick = item.get("top_pick", "-")
        if leader_regime == "고정 대장" and appearances >= 3 and avg_smart > 0:
            action = "섹터 추적"
            reason = f"{top_pick}이(가) {appearances}일 중 {item.get('top_pick_count', 0)}일 선두였고 평균 score {score}, 평균 스마트머니 {avg_smart}억이 유지됐습니다."
        elif leader_regime == "반복 대장" or appearances >= 2:
            action = "대장주 중심"
            reason = f"섹터 반복성은 보이지만 대장 고정도는 중간이라 `{top_pick}` 중심 확인이 더 중요합니다."
        else:
            action = "단기 확인"
            alt_pick = item.get("alt_pick") or "-"
            reason = f"최근 상단에는 올라왔지만 `{top_pick}`과 `{alt_pick}` 사이 선두 교체 가능성이 있어 하루 더 확인하는 편이 낫습니다."
        if universe_status == "재점검":
            if action == "섹터 추적":
                action = "대장주 중심"
            elif action == "대장주 중심":
                action = "단기 확인"
            reason += " 다만 현재 유니버스가 `재점검` 상태라 섹터 전체보다 대표주 확인이 먼저입니다."
        elif universe_status == "유동형" and action == "섹터 추적":
            reason += " 현재 유니버스가 `유동형`이라 섹터 추적 시에도 선두 종목 유지 여부를 같이 봐야 합니다."
        watch.append(
            {
                "sector": item.get("sector_name"),
                "leaders": [item.get("top_pick") or "-"],
                "action": action,
                "reason": reason,
                "leader_regime": leader_regime,
                "universe_status_label": universe_status,
            }
        )
    for item in risk_sectors[:2]:
        caution.append(
            {
                "sector": item.get("sector_name"),
                "leaders": [item.get("top_pick") or "-"],
                "action": "보수적",
                "reason": f"{int(item.get('appearances', 0))}일 반복 경계 구간이었고 평균 스마트머니가 {round(float(item.get('smart_money_net', 0)), 1)}억입니다.",
                "leader_regime": item.get("leader_regime") or "순환형",
                "universe_status_label": item.get("universe_status_label") or "",
            }
        )
    return {"watch": watch, "caution": caution}


def _build_rotation_structured_summary(day_snapshots):
    sector_stats = defaultdict(lambda: {"appearances": 0, "score_sum": 0.0, "smart_sum": 0.0, "leaders": Counter(), "actors": Counter()})
    risk_stats = defaultdict(lambda: {"appearances": 0, "score_sum": 0.0, "smart_sum": 0.0, "leaders": Counter(), "actors": Counter()})
    leader_counts = Counter()
    phase_counts = Counter()

    for snapshot in day_snapshots:
        phase_counts[str(snapshot.get("market_phase") or "unknown")] += 1
        for sector in snapshot.get("top_rotation", []):
            sector_name = sector.get("sector_name")
            if not sector_name:
                continue
            stat = sector_stats[sector_name]
            stat["appearances"] += 1
            stat["score_sum"] += float(sector.get("score", 0) or 0)
            stat["smart_sum"] += float(sector.get("smart_money_net", 0) or 0)
            if sector.get("top_pick"):
                stat["leaders"][sector["top_pick"]] += 1
                leader_counts[sector["top_pick"]] += 1
            if sector.get("dominant_actor"):
                stat["actors"][sector["dominant_actor"]] += 1

        for sector in snapshot.get("risk_sectors", []):
            sector_name = sector.get("sector_name")
            if not sector_name:
                continue
            stat = risk_stats[sector_name]
            stat["appearances"] += 1
            stat["score_sum"] += float(sector.get("score", 0) or 0)
            stat["smart_sum"] += float(sector.get("smart_money_net", 0) or 0)
            if sector.get("top_pick"):
                stat["leaders"][sector["top_pick"]] += 1
            if sector.get("dominant_actor"):
                stat["actors"][sector["dominant_actor"]] += 1

    top_rotation_sectors = _aggregate_sector_rows(
        sorted(
            sector_stats.items(),
            key=lambda item: (item[1]["appearances"], item[1]["score_sum"], item[1]["smart_sum"]),
            reverse=True,
        )[:6]
    )
    risk_sectors = _aggregate_sector_rows(
        sorted(
            risk_stats.items(),
            key=lambda item: (item[1]["appearances"], item[1]["score_sum"], -item[1]["smart_sum"]),
            reverse=True,
        )[:4],
        risk=True,
    )
    repeat_leaders = [
        {"name": name, "appearances": count}
        for name, count in leader_counts.most_common(5)
    ]
    dominant_theme = _dominant_rotation_theme(top_rotation_sectors)
    market_mode = _rotation_market_mode(top_rotation_sectors)
    top_repeat = int(top_rotation_sectors[0].get("appearances", 0)) if top_rotation_sectors else 0
    repeat_leader = int(repeat_leaders[0].get("appearances", 0)) if repeat_leaders else 0
    phase_focus = phase_counts.most_common(1)[0][1] if phase_counts else 0
    latest_universe_summary = day_snapshots[-1].get("universe_summary") if day_snapshots else {}
    latest_universe_status_map = day_snapshots[-1].get("universe_status_map") if day_snapshots else {}
    confidence_score = 38 + min(18, len(day_snapshots) * 3) + min(18, top_repeat * 5) + min(12, repeat_leader * 4) + min(8, phase_focus * 2)
    if len(top_rotation_sectors) <= 1:
        confidence_score -= 8
    confidence_score -= min(10, int((latest_universe_summary or {}).get("review_sector_count", 0)) * 2)
    if (latest_universe_summary or {}).get("history_confidence_label") == "예비":
        confidence_score -= 4
    confidence_score = max(0, min(100, int(confidence_score)))
    for rows in (top_rotation_sectors, risk_sectors):
        for item in rows:
            status = latest_universe_status_map.get(_clean_sector_name(item.get("sector_name")))
            if status:
                item["universe_status_label"] = status.get("label", "")
                item["universe_status_reason"] = status.get("reason", "")
    ideas = _build_rotation_ideas(top_rotation_sectors, risk_sectors)

    if top_rotation_sectors and risk_sectors:
        rotation_line = (
            f"`{top_rotation_sectors[0]['sector_short']}` 중심 순환매가 반복되는 가운데 "
            f"`{risk_sectors[0]['sector_short']}` 쪽은 개인 역행 가능성을 경계할 구간입니다."
        )
    elif top_rotation_sectors:
        rotation_line = (
            f"`{top_rotation_sectors[0]['sector_short']}` 쪽으로 자금이 반복적으로 모였고 "
            f"`{top_rotation_sectors[0]['top_pick']}` 확인이 가장 우선입니다."
        )
    else:
        rotation_line = "순환매의 반복 중심축이 아직 뚜렷하지 않습니다."

    return {
        "top_rotation_sectors": top_rotation_sectors,
        "risk_sectors": risk_sectors,
        "repeat_leaders": repeat_leaders,
        "market_phase_counts": dict(phase_counts),
        "dominant_theme": dominant_theme,
        "market_mode": market_mode,
        "confidence_score": confidence_score,
        "rotation_line": rotation_line,
        "watch_ideas": ideas.get("watch", []),
        "caution_ideas": ideas.get("caution", []),
        "universe_summary": latest_universe_summary or {},
        "universe_regime": (latest_universe_summary or {}).get("universe_regime", ""),
    }


def _build_rotation_structured_context(summary):
    if not summary:
        return ""
    lines = [
        "[사전 구조화 요약]",
        f"- 시장 모드: {summary.get('market_mode', '중립')} | 확신도: {summary.get('confidence_score', 0)}/100 | 중심 테마: {summary.get('dominant_theme', '-')}",
        f"- 한 줄 요약: {summary.get('rotation_line', '-')}",
    ]
    top_rotation = summary.get("top_rotation_sectors") or []
    if top_rotation:
        lines.append("- 반복 상위 섹터:")
        for item in top_rotation[:3]:
            lines.append(
                "  - "
                f"{item.get('sector_name')} | {item.get('appearances', 0)}일 | "
                f"avg score {item.get('score', 0)} | avg 스마트머니 {item.get('smart_money_net', 0)}억 | "
                f"대장 {item.get('top_pick', '-')} ({item.get('leader_regime', '순환형')}) | "
                f"유니버스 {item.get('universe_status_label', '-') or '-'}"
            )
    risk_sectors = summary.get("risk_sectors") or []
    if risk_sectors:
        lines.append("- 반복 경계 섹터:")
        for item in risk_sectors[:2]:
            lines.append(
                "  - "
                f"{item.get('sector_name')} | {item.get('appearances', 0)}일 | "
                f"avg 스마트머니 {item.get('smart_money_net', 0)}억 | "
                f"대장 {item.get('top_pick', '-')} ({item.get('leader_regime', '순환형')}) | "
                f"유니버스 {item.get('universe_status_label', '-') or '-'}"
            )
    repeat_leaders = summary.get("repeat_leaders") or []
    if repeat_leaders:
        leader_text = ", ".join(
            f"{item.get('name')} {item.get('appearances')}회"
            for item in repeat_leaders[:4]
            if item.get("name")
        )
        if leader_text:
            lines.append(f"- 반복 대장주: {leader_text}")
    universe_summary = summary.get("universe_summary") or {}
    if universe_summary:
        lines.append(
            f"- 유니버스 조정: 동적 편입 {universe_summary.get('dynamic_symbol_count', 0)}개 | "
            f"섹터 불일치 제외 {universe_summary.get('mismatch_symbol_count', 0)}개"
        )
        lines.append(
            f"- 유니버스 안정도: 안정형 {universe_summary.get('stable_sector_count', 0)}개 | "
            f"유동형 {universe_summary.get('adaptive_sector_count', 0)}개 | "
            f"재점검 {universe_summary.get('review_sector_count', 0)}개 | "
            f"상태 {universe_summary.get('universe_regime', '-')} | "
            f"평균 겹침률 {universe_summary.get('history_avg_overlap', 1.0)} | "
            f"표본 {universe_summary.get('history_confidence_label', '없음')} "
            f"({universe_summary.get('history_day_count', 1)}일)"
        )
        turnover = universe_summary.get("turnover") or {}
        if turnover:
            lines.append(
                f"- 전회 대비: 새 편입 {turnover.get('added_symbol_count', 0)}개 | "
                f"제외 {turnover.get('removed_symbol_count', 0)}개"
            )
        review_rows = universe_summary.get("review_sectors") or []
        if review_rows:
            review_text = ", ".join(row.get("sector") for row in review_rows[:3] if row.get("sector"))
            if review_text:
                lines.append(f"- 재점검 섹터: {review_text}")
    return "\n".join(lines)


def _build_rotation_context(days=30):
    log_dir = os.path.join(current_dir, "logs")
    day_snapshots = []
    sector_stats = defaultdict(lambda: {"appearances": 0, "score_sum": 0, "smart_sum": 0, "leaders": Counter()})
    risk_stats = Counter()

    for i in range(days, -1, -1):
        date_str = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
        file_path = os.path.join(log_dir, f"wics_log_{date_str}.json")
        if not os.path.exists(file_path):
            continue

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                log_data = json.load(f)
        except Exception as exc:
            log.warning("로그 읽기 에러 (%s): %s", date_str, exc)
            continue

        if not log_data:
            continue

        latest_entry = log_data[-1]
        snapshot = _build_day_snapshot(date_str, latest_entry)
        day_snapshots.append(snapshot)

        for sector in snapshot["top_rotation"]:
            sector_name = sector.get("sector_name")
            if not sector_name:
                continue
            stat = sector_stats[sector_name]
            stat["appearances"] += 1
            stat["score_sum"] += sector.get("score", 0)
            stat["smart_sum"] += sector.get("smart_money_net", 0)
            if sector.get("top_pick"):
                stat["leaders"][sector["top_pick"]] += 1

        for sector in snapshot["risk_sectors"]:
            if sector.get("sector_name"):
                risk_stats[sector["sector_name"]] += 1

    if not day_snapshots:
        return "", [], {}, {}

    lines = ["[WICS 기간 요약]"]
    lines.append(f"- 커버 일수: {len(day_snapshots)}일")
    lines.append("- 일자별 상위 섹터:")
    for snapshot in day_snapshots[-10:]:
        top_text = " / ".join(
            f"{sector.get('sector_name')}({sector.get('top_pick', '-')}) score {sector.get('score', 0)}"
            for sector in snapshot.get("top_rotation", [])
        ) or "데이터 없음"
        risk_sector = snapshot.get("risk_sectors", [])
        risk_text = risk_sector[0]["sector_name"] if risk_sector else "-"
        lines.append(f"  {snapshot['date']}: {top_text} | 위험 {risk_text}")

    ranked_sectors = sorted(
        sector_stats.items(),
        key=lambda item: (
            item[1]["appearances"],
            item[1]["score_sum"],
            item[1]["smart_sum"],
        ),
        reverse=True,
    )
    lines.append("- 누적 상위 섹터:")
    for sector_name, stat in ranked_sectors[:6]:
        avg_score = round(stat["score_sum"] / stat["appearances"], 1) if stat["appearances"] else 0
        avg_smart = round(stat["smart_sum"] / stat["appearances"], 1) if stat["appearances"] else 0
        leader_text = ", ".join(stock for stock, _ in stat["leaders"].most_common(2)) or "-"
        lines.append(
            f"  {sector_name}: 등장 {stat['appearances']}일 | 평균 score {avg_score} | 평균 스마트머니 {avg_smart}억 | 반복 대장 {leader_text}"
        )

    if risk_stats:
        lines.append("- 반복 리스크 섹터: " + ", ".join(f"{name} {count}일" for name, count in risk_stats.most_common(3)))

    latest_summaries = [snapshot["raw_summary"] for snapshot in day_snapshots[-3:] if snapshot.get("raw_summary")]
    structured_summary = _build_rotation_structured_summary(day_snapshots)
    return "\n".join(lines), day_snapshots, latest_summaries, structured_summary


def _count_rotation_files(days=30):
    log_dir = os.path.join(current_dir, "logs")
    count = 0
    for i in range(days, -1, -1):
        date_str = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
        file_path = os.path.join(log_dir, f"wics_log_{date_str}.json")
        if os.path.exists(file_path):
            count += 1
    return count


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


def _fallback_rotation_report(rotation_context, start_date, end_date, files_found, structured_summary=None):
    structured_summary = structured_summary or {}
    universe_summary = structured_summary.get("universe_summary") or {}
    lines = [
        "🌪️ *[AI 퀀트: 월간/주간 순환매 추적 리포트]*",
        f"🗓️ *분석 기간:* {start_date} ~ {end_date} (총 {files_found}일 치 데이터 병합)",
        "",
        "*0. 시장 한 줄 요약*",
        f"- 시장 모드 `{structured_summary.get('market_mode', '중립')}` | 확신도 `{structured_summary.get('confidence_score', 0)}/100` | 중심 테마 `{structured_summary.get('dominant_theme', '-') or '-'}`",
        f"- {structured_summary.get('rotation_line', '반복 순환매 중심축은 아직 제한적입니다.')}",
        "",
        "*1. 관측 사실*",
    ]
    if universe_summary:
        lines.insert(
            6,
            f"- 유니버스 안정도 `안정형 {universe_summary.get('stable_sector_count', 0)} / "
            f"유동형 {universe_summary.get('adaptive_sector_count', 0)} / "
            f"재점검 {universe_summary.get('review_sector_count', 0)}` | "
            f"상태 `{universe_summary.get('universe_regime', '-')}` | "
            f"표본 `{universe_summary.get('history_confidence_label', '없음')}` "
            f"({universe_summary.get('history_day_count', 1)}일)",
        )
    top_rotation = structured_summary.get("top_rotation_sectors") or []
    if top_rotation:
        for item in top_rotation[:3]:
            lines.append(
                f"- `{item.get('sector_name')}` | 반복 {item.get('appearances', 0)}일 | "
                f"avg score {item.get('score', 0)} | "
                f"대장 `{item.get('top_pick', '-')}` ({item.get('leader_regime', '순환형')}) | "
                f"유니버스 `{item.get('universe_status_label', '-') or '-'}`"
            )
    risk_sectors = structured_summary.get("risk_sectors") or []
    if risk_sectors:
        lines.append("- 경계 섹터: " + " / ".join(
            f"{item.get('sector_name')}({item.get('top_pick', '-')})"
            for item in risk_sectors[:2]
            if item.get("sector_name")
        ))
    lines.extend(
        [
            "",
            "*2. 추정*",
        ]
    )
    for item in (structured_summary.get("watch_ideas") or [])[:3]:
        lines.append(
            f"- `{item.get('sector')}` | {item.get('action', '-')}"
            f" | 대장 흐름 `{item.get('leader_regime', '순환형')}`"
            f" | 유니버스 `{item.get('universe_status_label', '-') or '-'}` | {item.get('reason', '')}"
        )
    if structured_summary.get("repeat_leaders"):
        leader_text = ", ".join(
            f"{item.get('name')} {item.get('appearances')}회"
            for item in structured_summary.get("repeat_leaders", [])[:3]
        )
        lines.append(f"- 반복 대장주: {leader_text}")
    lines.extend(
        [
            "",
            "*3. 미확인/주의*",
            f"- {_ai_unavailable_reason()}",
            "- 키를 코드로 관리하려면 `Disclosure/config.py`의 `HARDCODED_GEMINI_API_KEY`에 직접 넣으면 됩니다.",
            "- 키를 PM2로 관리하려면 `ecosystem.config.js`의 `GEMINI_API_KEY`를 채운 뒤 재시작하면 됩니다.",
            "- 아래는 AI 해설 없이도 바로 읽을 수 있도록 정리한 기간 요약입니다.",
            rotation_context,
        ]
    )
    return "\n".join(lines)


def _save_rotation_report(report_text, days, files_found, structured_summary=None):
    os.makedirs(REPORT_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    structured_summary = structured_summary or {}
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "lookback_days": int(days),
        "files_found": int(files_found),
        "report_text": str(report_text or ""),
        "top_rotation_sectors": structured_summary.get("top_rotation_sectors", []),
        "risk_sectors": structured_summary.get("risk_sectors", []),
        "repeat_leaders": structured_summary.get("repeat_leaders", []),
        "market_phase_counts": structured_summary.get("market_phase_counts", {}),
        "dominant_theme": structured_summary.get("dominant_theme", ""),
        "market_mode": structured_summary.get("market_mode", "중립"),
        "confidence_score": int(structured_summary.get("confidence_score", 0) or 0),
        "rotation_line": structured_summary.get("rotation_line", ""),
        "watch_ideas": structured_summary.get("watch_ideas", []),
        "caution_ideas": structured_summary.get("caution_ideas", []),
        "universe_summary": structured_summary.get("universe_summary", {}),
        "universe_regime": structured_summary.get("universe_regime", ""),
        "ai_enabled": bool(GEMINI_API_KEY and (google_genai is not None or google_generativeai is not None)),
        "ai_unavailable_reason": "" if (GEMINI_API_KEY and (google_genai is not None or google_generativeai is not None)) else _ai_unavailable_reason(),
    }
    report_json = os.path.join(REPORT_DIR, f"wics_ai_report_{stamp}.json")
    latest_json = os.path.join(REPORT_DIR, "wics_ai_report_latest.json")
    report_txt = os.path.join(REPORT_DIR, f"wics_ai_report_{stamp}.txt")
    latest_txt = os.path.join(REPORT_DIR, "wics_ai_report_latest.txt")
    with open(report_json, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
    with open(latest_json, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
    with open(report_txt, "w", encoding="utf-8") as fp:
        fp.write(str(report_text or ""))
    with open(latest_txt, "w", encoding="utf-8") as fp:
        fp.write(str(report_text or ""))

async def analyze_rotation_logs(days=30):
    """최대 설정된 기간(기본 30일) 동안의 WICS 로그를 긁어모아 AI에게 던집니다."""
    rotation_context, day_snapshots, recent_raw_summaries, structured_summary = _build_rotation_context(days)

    if not day_snapshots:
        return "수집된 WICS 로그 데이터가 없습니다. 모니터링 봇을 먼저 가동하여 로그를 쌓아주세요.", {}

    start_date = day_snapshots[0]["date"]
    end_date = day_snapshots[-1]["date"]
    files_found = len(day_snapshots)

    # 4. 메가 트렌드 분석 프롬프트 (분석 기간 주입)
    prompt_instruction = f"""
    [특명] 당신은 여의도 최상위 프랍 트레이딩 데스크의 '메가 트렌드 & 순환매(Sector Rotation) 수석 애널리스트'입니다.
    
    아래 제공된 최대 1개월 치의 'WICS 26 섹터 및 개별종목 수급 데이터(단위: 억원, 등락률: %)'를 심층 분석하여,
    한국 증시의 거대한 자본 이동 흐름을 완벽하게 해독해 내야 합니다.
    특히 하루짜리 노이즈보다 '여러 날 반복 등장한 섹터'와 '같이 반복 등장한 대장주'를 더 높은 우선순위로 봐야 합니다.

    [분석 핵심 요구사항 🎯]
    1. 🔄 자본의 대이동 (순환매 포착): 외국인과 기관의 자금이 '어느 섹터에서 빠져나와, 어느 섹터로 흘러 들어갔는지' 흐름을 짚어주세요.
    2. 🔥 넥스트 주도 섹터 & 핵심 종목: 현재 매집이 가장 강력하게 누적되고 있으며, 다음 주(또는 내일) 시장을 주도할 1순위 섹터와 그 안에서 가장 돈이 많이 몰린 대장주 2~3개를 픽해주세요.
    3. 🌍 매크로 요인 추론: 현재 수급이 몰리는 섹터를 바탕으로 현재 시장을 지배하는 '매크로 핵심 테마'가 무엇인지 역추론하여 1줄로 요약해 주세요.
    4. ⚠️ 소외/위험 섹터: 외인/기관의 무자비한 이탈과 개인의 물타기만 진행 중인 가장 위험한 섹터 1개를 경고해 주세요.
    5. 출력은 반드시 `관측 사실 / 추정 / 미확인`을 분리하세요.
    6. 섹터와 종목 이름을 길게 나열하지 말고, 반복 등장한 중심축 위주로 3개 안팎만 남기세요.
    7. 보고서처럼 쓰지 말고, 실제 운용자가 읽는 브리핑처럼 써주세요.

    [출력 형식]
    * 반드시 슬랙(Slack) 마크다운을 사용하여 가독성 높게 작성할 것.
    
    🌪️ *[AI 퀀트: 월간/주간 순환매 추적 리포트]*
    🗓️ *분석 기간:* {start_date} ~ {end_date} (총 {files_found}일 치 데이터 병합)

    *0. 시장 한 줄 요약*
    - 이번 구간의 중심 순환매를 한 문장으로 정리

    *1. 관측 사실*
    - *자금 이탈:* [섹터명] ➡️ *자금 유입:* [섹터명]
    - 반복 등장한 섹터 / 반복 대장주 / score 근거

    *2. 추정*
    - 현재 시장을 지배하는 매크로 테마 1~2줄
    - 넥스트 주도 섹터와 Top Pick 종목 해석
    - 지금은 섹터 전체를 살지, 대장주만 볼지까지 짧게 설명

    *3. 미확인/주의*
    - 데이터 한계, 반대 시나리오, 위험 섹터

    *4. 🔥 넥스트 주도 섹터 & Top Pick 종목*
    - *주도 섹터:* - *핵심 주도주:* (종목명 및 선정 이유, 수급 누적 상태 언급)
    """

    raw_summary_excerpt = ""
    if recent_raw_summaries:
        raw_summary_excerpt = "\n\n[최근 요약 원본]\n" + json.dumps(recent_raw_summaries, ensure_ascii=False)

    structured_context = _build_rotation_structured_context(structured_summary)
    prompt = f"{prompt_instruction}\n\n[WICS 수급 기간 요약]\n{rotation_context}\n\n{structured_context}{raw_summary_excerpt}"

    try:
        response_text = await _generate_ai_text(prompt)
        if response_text:
            return response_text, structured_summary
        log.warning("Gemini client unavailable; sending fallback rotation digest.")
        return _fallback_rotation_report(rotation_context, start_date, end_date, files_found, structured_summary), structured_summary
    except Exception as e:
        log.warning("Rotation AI analysis failed: %s", e)
        return _fallback_rotation_report(rotation_context, start_date, end_date, files_found, structured_summary), structured_summary

async def run_rotation_scheduler():
    print("🤖 AI 순환매(Sector Rotation) 분석 리포터 대기 중...")
    
    while True:
        now = datetime.now()
        
        # 💡 매일 장 마감 후 16시 00분에 '당일 포함 최근 한 달 순환매 리포트' 발송
        if (now.hour == 15 and now.minute == 40) or (now.hour == 20 and now.minute == 10):
            print(f"[{now.strftime('%H:%M')}] 🌪️ 1개월 치 데이터 기반 순환매 분석 중...")
            report, structured_summary = await analyze_rotation_logs(days=30)
            _save_rotation_report(report, days=30, files_found=_count_rotation_files(30), structured_summary=structured_summary)
            send_slack(report, title="[일간] 🤖 AI 스마트머니 순환매 & 매크로 분석", msg_type="warning")
            await asyncio.sleep(60) # 중복 발송 방지
            
        # 💡 원하신다면 매주 금요일 저녁 주간 마감 브리핑 등의 조건을 추가할 수도 있습니다.
        # if now.weekday() == 4 and now.hour == 18 and now.minute == 0: ...

        await asyncio.sleep(30) # 30초마다 시간 체크

if __name__ == "__main__":
    # 테스트용: 스크립트 실행 시 즉시 1번 분석 리포트를 뽑아보고 싶다면 아래 주석을 해제하세요.
    # report = asyncio.run(analyze_rotation_logs(days=30))
    # send_slack(report, title="[테스트] AI 순환매 분석", msg_type="info")
    
    # 스케줄러 가동
    asyncio.run(run_rotation_scheduler())
