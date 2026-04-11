from __future__ import annotations


def _is_valid_number(value) -> bool:
    if value is None:
        return False
    try:
        return str(value).strip().lower() not in {"", "nan", "none", "<na>"}
    except Exception:
        return False


def _to_float(value, default: float = 0.0) -> float:
    if not _is_valid_number(value):
        return default
    try:
        return float(value)
    except Exception:
        return default


def _fmt_num(value, digits: int = 4) -> str:
    if not _is_valid_number(value):
        return "-"
    return f"{_to_float(value):.{digits}f}"


def _fmt_pct(value, digits: int = 2) -> str:
    if not _is_valid_number(value):
        return "-"
    return f"{_to_float(value):.{digits}f}%"


def _fmt_price(value) -> str:
    if not _is_valid_number(value):
        return "-"
    return f"{int(round(_to_float(value))):,}"


def _event_path_take(item: dict) -> str:
    profile = str(item.get("event_reaction_profile") or "").strip()
    avg1d = _to_float(item.get("event_expected_alpha_1d"))
    avg3d = _to_float(item.get("event_expected_alpha_3d"))
    avg5d = _to_float(item.get("event_expected_alpha_5d"))
    if profile == "초기 반응과 유지력":
        return "초기 반응과 5일 유지력이 함께 우호적이었습니다."
    if profile == "초기 반응 후 둔화":
        return "초기 반응은 있었지만 3일~5일 구간 유지력은 약해졌습니다."
    if profile == "시간차 반응":
        return "즉시 반응보다 하루 이틀 지난 뒤 반응이 붙는 쪽에 가깝습니다."
    if profile == "반응 약함":
        return "초기 반응과 5일 성과 모두 강하다고 보긴 어렵습니다."
    if avg1d > 0 or avg3d > 0 or avg5d > 0:
        return "초기 반응과 유지력을 함께 더 확인할 필요가 있습니다."
    return "사후 성과 패턴은 아직 뚜렷하지 않습니다."


def _top_names(items: list[dict], limit: int = 3) -> str:
    names = []
    for item in items[:limit]:
        name = str(item.get("name") or "").strip()
        symbol = str(item.get("symbol") or "").strip()
        if name:
            names.append(name)
        elif symbol:
            names.append(symbol)
    return ", ".join(names) if names else "-"


def _top_sector_names(items: list[dict], limit: int = 3) -> str:
    names = [str(item.get("sector") or "").strip() for item in items[:limit] if str(item.get("sector") or "").strip()]
    return ", ".join(names) if names else "-"


def _display_label(item: dict) -> str:
    name = str(item.get("name") or "").strip()
    symbol = str(item.get("symbol") or "").strip()
    if name and symbol:
        return f"{name}({symbol})"
    return name or symbol or "-"


def _build_strengths(item: dict) -> list[str]:
    strengths = []
    flow_confidence = _to_float(item.get("flow_confidence_score"), 1.0)
    flow_fallback_used = bool(item.get("flow_fallback_used"))
    if _to_float(item.get("composite_score")) >= 0.65:
        strengths.append("팩터 체력이 상위권입니다")
    if _to_float(item.get("analyst_conviction_score")) >= 7.0:
        strengths.append("애널 컨센서스가 강하게 받쳐줍니다")
    if _to_float(item.get("flow_state_score")) >= 120:
        if flow_fallback_used or flow_confidence < 0.85:
            strengths.append("보강 수급 기준으로도 흐름이 따라오고 있습니다")
        else:
            strengths.append("실제 수급도 두껍게 붙어 있습니다")
    elif _to_float(item.get("flow_state_score")) >= 40:
        if flow_fallback_used or flow_confidence < 0.85:
            strengths.append("보강 수급 기준으로는 흐름이 보조적으로 따라옵니다")
        else:
            strengths.append("실제 수급이 보조적으로 따라오고 있습니다")
    if _to_float(item.get("flow_intraday_edge_score")) >= 0.25:
        strengths.append("장중 타이밍도 살아 있습니다")
    if str(item.get("event_interpretation_label") or "") == "참고 가능" and _to_float(item.get("event_alpha_score")) >= 0:
        strengths.append("최근 공시 이벤트도 우호적입니다")
    if _to_float(item.get("microstructure_score")) >= 0.35:
        strengths.append("미시구조가 아직 무너지지 않았습니다")
    history_label = str(item.get("wics_history_confidence_label") or "")
    if _to_float(item.get("alignment_score")) >= 1 and history_label == "충분":
        strengths.append("시장 큰 흐름과도 같은 방향입니다")
    elif _to_float(item.get("alignment_score")) >= 1 and history_label in {"예비", "없음"}:
        strengths.append("시장 큰 흐름과 대체로 같은 방향으로 읽히지만 WICS 표본은 아직 얕습니다")
    if str(item.get("sector_final_label") or "") == "공통 우호":
        strengths.append("섹터 해석도 세 렌즈가 함께 받쳐주는 편입니다")
    elif str(item.get("sector_final_label") or "") == "체력 우위":
        strengths.append("섹터 안에서 체력이 남은 종목을 찾는 쪽에 유리한 판입니다")
    if str(item.get("universe_status_label") or "") == "안정형":
        strengths.append("WICS 바스켓도 비교적 안정적으로 유지되는 편입니다")
    if _is_valid_number(item.get("fair_value_gap_pct")) and _to_float(item.get("fair_value_confidence_score")) >= 0.55:
        gap_pct = _to_float(item.get("fair_value_gap_pct"))
        if gap_pct >= 10.0:
            strengths.append("기준 적정가 대비 할인 폭이 아직 남아 있습니다")
    return strengths


def _build_risks(item: dict) -> list[str]:
    risks = []
    flow_confidence = _to_float(item.get("flow_confidence_score"), 1.0)
    if _to_float(item.get("flow_intraday_edge_score")) <= -0.15:
        risks.append("장중 추격 신호는 약합니다")
    if _to_float(item.get("event_alpha_score")) <= -0.1:
        risks.append("최근 공시 재료는 역풍 쪽에 가깝습니다")
    event_label = str(item.get("event_interpretation_label") or "")
    if event_label == "해석 보류":
        risks.append("공시 백테스트는 아직 5일 성과 표본이 없어 해석을 보류하는 편이 낫습니다")
    elif event_label == "표본 얕음":
        risks.append("공시 백테스트 표본이 얕아 과신보다 관찰이 먼저입니다")
    elif event_label == "변동성 주의":
        risks.append("공시 통계는 괜찮지만 중간 흔들림이 큰 편입니다")
    if _is_valid_number(item.get("analyst_target_upside_pct")) and _to_float(item.get("analyst_target_upside_pct")) < 0:
        risks.append("애널 목표가 upside가 제한적입니다")
    if _to_float(item.get("microstructure_score")) < 0:
        risks.append("미시구조가 약해 흔들릴 수 있습니다")
    if int(item.get("active_source_count", 0) or 0) <= 3:
        risks.append("겹치는 신호 수가 아직 많지 않습니다")
    if _to_float(item.get("alignment_score")) <= -1:
        risks.append("시장 큰 흐름과는 약간 어긋납니다")
    if str(item.get("wics_history_confidence_label") or "") in {"예비", "없음"} and _to_float(item.get("alignment_score")) != 0:
        risks.append("WICS 히스토리 표본이 얕아 섹터 해석은 보조적으로 보는 편이 낫습니다")
    if str(item.get("sector_final_label") or "") == "단기 순환 우세":
        risks.append("섹터 흐름은 빠르지만 체력 확인 전 추격은 보수적으로 보는 편이 낫습니다")
    if str(item.get("sector_final_label") or "") == "매크로 우호":
        risks.append("환경은 우호적이지만 수급이 약할 수 있어 먼저 관찰하는 편이 낫습니다")
    if str(item.get("sector_final_label") or "") == "보류":
        risks.append("섹터 공통 결론도 아직 보류 쪽이라 종목 단독 추격은 더 조심해야 합니다")
    if bool(item.get("flow_fallback_used")) or flow_confidence < 0.8:
        risks.append("투자자 수급은 보강 데이터 비중이 있어 평소보다 보수적으로 읽는 편이 낫습니다")
    if str(item.get("universe_status_label") or "") == "재점검":
        risks.append("WICS 바스켓 편출입이 잦아 섹터 단위 과신은 줄이는 편이 낫습니다")
    if int(item.get("wics_dynamic_count", 0) or 0) > 0 and _to_float(item.get("wics_dynamic_stability")) < 0.7:
        risks.append("동적 후보는 있었지만 안정도가 낮아 섹터 확신은 한 단계 낮춰 보는 편이 낫습니다")
    if _is_valid_number(item.get("fair_value_gap_pct")) and _to_float(item.get("fair_value_confidence_score")) >= 0.55:
        gap_pct = _to_float(item.get("fair_value_gap_pct"))
        if gap_pct <= -10.0:
            risks.append("기준 적정가 대비 선반영 구간이라 가격 부담을 먼저 봐야 합니다")
    return risks


def _card_action(item: dict) -> tuple[str, str]:
    active = int(item.get("active_source_count", 0) or 0)
    card_score = _to_float(item.get("card_score"))
    intraday = _to_float(item.get("flow_intraday_edge_score"))
    event_alpha = _to_float(item.get("event_alpha_score"))
    flow = _to_float(item.get("flow_state_score"))
    regime = str(item.get("decision_regime") or "normal").lower()
    data_quality = str(item.get("data_quality_label") or "").strip()
    alignment = int(_to_float(item.get("alignment_score"), 0))
    wics_history_label = str(item.get("wics_history_confidence_label") or "")

    if active >= 6 and card_score >= 0.74 and intraday > -0.12 and event_alpha > -0.12:
        action = "직접 후보"
        conclusion = "여러 신호가 함께 받쳐줘 우선순위를 높게 둘 만합니다"
    elif card_score >= 0.68 and (intraday >= 0.2 or event_alpha >= 0.2 or flow >= 40):
        action = "눌림 후보"
        conclusion = "체력은 괜찮지만 타이밍 확인 후 접근하는 편이 낫습니다"
    elif active >= 4 or intraday >= 0.1 or event_alpha >= 0.1:
        action = "관찰 후보"
        conclusion = "재료는 있으나 단독 추격보다 확인이 먼저입니다"
    else:
        action = "보류"
        conclusion = "지금 바로 매수 판단까지 이어지기에는 근거가 더 필요합니다"

    if regime == "confirm_first":
        if action == "직접 후보":
            action = "눌림 후보"
            conclusion = "기본 체력은 괜찮지만 실시간 수급 확인 전에는 한 단계 보수적으로 보는 편이 낫습니다"
        elif action == "눌림 후보":
            action = "관찰 후보"
            conclusion = "신호는 있지만 실시간 확인 전에는 관찰 단계로 두는 편이 낫습니다"
    elif regime == "observe_only" and action in {"직접 후보", "눌림 후보"}:
        action = "관찰 후보"
        conclusion = "데이터 공백이 있어 오늘은 관찰 중심으로 해석하는 편이 낫습니다"

    if data_quality == "보수적" and action == "직접 후보":
        action = "눌림 후보"
        conclusion = "점수는 높지만 데이터 품질이 보수적이라 바로 추격보다 확인 후 접근이 낫습니다"
    if alignment <= -2:
        if action == "직접 후보":
            action = "눌림 후보"
            conclusion = "종목 점수는 높지만 시장 큰 흐름과는 어긋나 보여 한 단계 보수적으로 해석하는 편이 낫습니다"
        elif action == "눌림 후보":
            action = "관찰 후보"
            conclusion = "재료는 있으나 시장 큰 흐름과 엇갈려 먼저 관찰로 두는 편이 낫습니다"
    elif alignment >= 1 and wics_history_label == "예비":
        if action == "직접 후보":
            action = "눌림 후보"
            conclusion = "종목 체력은 좋지만 WICS 히스토리 표본이 얕아 눌림 확인 후 접근하는 편이 낫습니다"
    elif alignment >= 1 and wics_history_label == "없음":
        if action == "직접 후보":
            action = "관찰 후보"
            conclusion = "종목 점수는 높지만 WICS 히스토리 표본이 거의 없어 우선 관찰로 두는 편이 낫습니다"
        elif action == "눌림 후보":
            action = "관찰 후보"
            conclusion = "재료는 있으나 WICS 히스토리 표본이 거의 없어 먼저 관찰하는 편이 낫습니다"

    sector_final_label = str(item.get("sector_final_label") or "")
    sector_action_hint = str(item.get("sector_action_hint") or "")
    if sector_final_label == "보류":
        if action == "직접 후보":
            action = "관찰 후보"
        elif action == "눌림 후보":
            action = "관찰 후보"
        conclusion = "종목 점수는 남아 있지만 섹터 공통 결론이 보류라 우선 관찰하는 편이 낫습니다"
    elif sector_final_label == "매크로 우호":
        if action in {"직접 후보", "눌림 후보"}:
            action = "관찰 후보"
            conclusion = "환경은 우호적이지만 수급 확인 전까지는 관찰 단계로 두는 편이 낫습니다"
    elif sector_final_label == "단기 순환 우세" and action == "직접 후보":
        action = "눌림 후보"
        conclusion = "수급은 뜨지만 단기 순환 성격이 강해 추격보다 눌림 확인이 먼저입니다"
    elif sector_final_label == "체력 우위" and action == "관찰 후보" and card_score >= 0.68:
        action = "눌림 후보"
        conclusion = "섹터 안에서 체력이 남은 종목을 보는 판이라 눌림 후보로 둘 만합니다"

    if sector_action_hint and conclusion:
        conclusion = f"{conclusion}. {sector_action_hint}"

    return action, conclusion


def render_card_lines(item: dict) -> list[str]:
    action, conclusion = _card_action(item)
    lines = [
        f"* {_display_label(item)} | {item['sector']} | 판단 `{action}` | 맥락 `{item.get('alignment_label') or '중립'}` | 섹터결론 `{item.get('sector_final_label') or '-'}` | 종합 `{_fmt_num(item['card_score'])}` | 겹치는 신호 `{int(item.get('active_source_count', 0) or 0)}`개"
    ]
    lines.append(f"  결론: {conclusion}.")
    if _is_valid_number(item.get("fair_value_base")):
        valuation_line = (
            f"  적정가: 현재가 `{_fmt_price(item.get('current_price'))}` | "
            f"보수 `{_fmt_price(item.get('fair_value_bear'))}` | "
            f"기준 `{_fmt_price(item.get('fair_value_base'))}` | "
            f"공격 `{_fmt_price(item.get('fair_value_bull'))}` | "
            f"괴리 `{_fmt_pct(item.get('fair_value_gap_pct'), 1)}` | "
            f"산출 신뢰 `{int(round(_to_float(item.get('fair_value_confidence_score')) * 100.0))}/100`"
        )
        if item.get("valuation_primary_method"):
            valuation_line += f" | 주요 근거 `{item.get('valuation_primary_method')}`"
        lines.append(valuation_line)
        if item.get("valuation_summary_paragraph"):
            lines.append(f"  적정가 해석: {item.get('valuation_summary_paragraph')}")
        elif item.get("valuation_reason_summary"):
            lines.append(f"  적정가 해석: {item.get('valuation_reason_summary')}")
    if item.get("sector_human_summary"):
        lines.append(
            f"  섹터 해석: 수급 `{_fmt_num(item.get('sector_flow_lens_score'), 1)}` | "
            f"퀀트 `{_fmt_num(item.get('sector_quant_lens_score'), 1)}` | "
            f"매크로 `{_fmt_num(item.get('sector_macro_lens_score'), 1)}` | "
            f"{item.get('sector_human_summary')}"
        )
    if item.get("universe_status_label"):
        dynamic_count = int(item.get("wics_dynamic_count", 0) or 0)
        history_label = item.get("wics_sector_history_confidence_label") or item.get("history_confidence_label") or "-"
        stability_text = "-" if dynamic_count <= 0 else f"{int(round(_to_float(item.get('wics_dynamic_stability')) * 100.0))}/100"
        universe_line = (
            f"  WICS 바스켓: 상태 `{item.get('universe_status_label')}` | "
            f"표본 `{history_label}` | "
            f"동적안정도 `{stability_text}`"
        )
        if item.get("universe_status_reason"):
            universe_line += f" | {item.get('universe_status_reason')}"
        lines.append(universe_line)
    if bool(item.get("flow_fallback_used")):
        lines.append("  수급 주의: 투자자 수급은 보강 데이터라 종합 판단에는 보수적으로 반영했습니다.")
    strengths = _build_strengths(item)
    risks = _build_risks(item)
    take_parts = []
    if strengths:
        take_parts.append(" / ".join(strengths[:3]) + ".")
    if risks:
        take_parts.append("다만 " + " / ".join(risks[:2]) + ".")
    if take_parts:
        lines.append("  해석: " + " ".join(take_parts))

    references = []
    if item.get("analyst_latest_title"):
        references.append(f"애널 `{item['analyst_latest_title']}`")
    if item.get("flow_top_brokers"):
        references.append(f"창구 `{item['flow_top_brokers']}`")
    if item.get("flow_event_mix"):
        references.append(f"흐름 `{item['flow_event_mix']}`")
    if item.get("event_recent_count"):
        references.append(
            f"공시 `{item.get('event_last_type', '-')}/{item.get('event_last_bias', '-')}` {int(item.get('event_recent_count', 0) or 0)}건"
        )
    if item.get("flow_intraday_samples"):
        references.append(
            f"장중 `{item['flow_intraday_last_event'] or '-'}` | 30분 {_fmt_pct(item.get('flow_intraday_avg_30m'), 2)} | 60분 {_fmt_pct(item.get('flow_intraday_avg_60m'), 2)}"
        )
    if references:
        lines.append("  참고: " + " | ".join(references))

    metrics = [
        f"factor `{_fmt_num(item.get('composite_score'))}`",
        f"analyst `{_fmt_num(item.get('analyst_conviction_score'))}`",
        f"flow `{_fmt_num(item.get('flow_state_score'), 1)}`",
        f"intraday `{_fmt_num(item.get('flow_intraday_edge_score'))}`",
        f"event `{_fmt_num(item.get('event_alpha_score'))}`",
        f"micro `{_fmt_num(item.get('microstructure_score'))}`",
    ]
    if _is_valid_number(item.get("flow_confidence_score")):
        metrics.append(f"수급확신 `{int(round(_to_float(item.get('flow_confidence_score')) * 100.0))}/100`")
    if _is_valid_number(item.get("ml_pred_score")) and _to_float(item.get("ml_pred_score")) != 0:
        metrics.append(f"ml `{_fmt_num(item.get('ml_pred_score'))}`")
    lines.append("  수치: " + " | ".join(metrics))
    return lines


def render_intraday_lines(leaders: list[dict], top_n: int) -> list[str]:
    if not leaders:
        return []
    lines = ["", "*3. 장중 타이밍 관찰*"]
    for item in leaders[: min(5, top_n)]:
        avg30 = _to_float(item.get("flow_intraday_avg_30m"))
        avg60 = _to_float(item.get("flow_intraday_avg_60m"))
        if avg30 > 0 and avg60 > 0:
            take = "짧은 구간과 한 시간 구간이 모두 우호적입니다"
        elif avg30 > 0:
            take = "초기 반응은 좋지만 한 시간 유지력은 더 확인이 필요합니다"
        else:
            take = "신호는 잡히지만 추격 매수 해석은 보수적으로 보는 편이 낫습니다"
        lines.append(
            f"* {_display_label(item)} | {item['sector']} | `{item['flow_intraday_last_event'] or '-'}` | "
            f"30분 `{_fmt_pct(item.get('flow_intraday_avg_30m'), 2)}` | 60분 `{_fmt_pct(item.get('flow_intraday_avg_60m'), 2)}` | 표본 `{int(item.get('flow_intraday_samples', 0) or 0)}`"
        )
        lines.append(f"  해석: {take}")
    return lines


def render_event_lines(leaders: list[dict], top_n: int) -> list[str]:
    if not leaders:
        return []
    lines = ["", "*4. 공시 이벤트 관찰*"]
    focus = [
        item for item in leaders
        if str(item.get("event_last_bias") or "").lower() == "positive"
        and str(item.get("event_tactical_label") or "") == "참고 가능"
    ]
    short_term = [
        item for item in leaders
        if str(item.get("event_last_bias") or "").lower() == "positive"
        and str(item.get("event_tactical_label") or "") == "단기 반응형"
    ]
    delayed = [
        item for item in leaders
        if str(item.get("event_last_bias") or "").lower() == "positive"
        and str(item.get("event_tactical_label") or "") == "지연 반응형"
    ]
    neutral = [
        item for item in leaders
        if str(item.get("event_tactical_label") or "") == "존재 확인"
    ]
    shallow = [
        item for item in leaders
        if str(item.get("event_last_bias") or "").lower() == "positive"
        and str(item.get("event_interpretation_label") or "") in {"표본 얕음", "해석 보류"}
        and str(item.get("event_tactical_label") or "") not in {"단기 반응형", "지연 반응형", "보수적 관찰", "존재 확인"}
    ]
    caution = [
        item for item in leaders
        if str(item.get("event_last_bias") or "").lower() == "negative"
        or str(item.get("event_interpretation_label") or "") in {"변동성 주의", "보수적"}
        or str(item.get("event_tactical_label") or "") == "보수적 관찰"
    ]
    if focus:
        lines.append(f"- 지금 공시만 놓고 봐도 `{_top_names(focus)}` 쪽은 바로 참고할 만합니다.")
    if short_term:
        lines.append(f"- `{_top_names(short_term)}` 쪽은 짧게만 보는 단기 반응형 재료입니다.")
    if delayed:
        lines.append(f"- `{_top_names(delayed)}` 쪽은 하루 이틀 뒤 확인하는 지연 반응형 재료입니다.")
    if neutral:
        lines.append(f"- `{_top_names(neutral)}` 쪽은 방향성보다 존재 확인이 먼저인 공시입니다.")
    if shallow:
        lines.append(f"- `{_top_names(shallow)}` 쪽은 재료는 보이지만 아직 표본이 얕습니다.")
    if caution:
        lines.append(f"- 반대로 `{_top_names(caution)}` 쪽은 단독 매수보다 보수적으로 보는 편이 낫습니다.")
    display_items = leaders[: min(5, top_n)]
    if display_items and not focus and not caution:
        unique_items = []
        seen_types: set[tuple[str, str]] = set()
        for item in display_items:
            key = (
                str(item.get("event_last_type") or "").upper(),
                str(item.get("event_last_bias") or "").lower(),
            )
            if key in seen_types:
                continue
            seen_types.add(key)
            unique_items.append(item)
            if len(unique_items) >= min(3, top_n):
                break
        if unique_items:
            skipped = max(0, len(display_items) - len(unique_items))
            display_items = unique_items
            if skipped > 0:
                lines.append(f"- 비슷한 해석이 반복되는 나머지 `{skipped}`건은 대표 사례만 남겼습니다.")
    for item in display_items:
        expected = _to_float(item.get("event_expected_alpha_5d"))
        expected_1d = _to_float(item.get("event_expected_alpha_1d"))
        expected_3d = _to_float(item.get("event_expected_alpha_3d"))
        bias = str(item.get("event_last_bias") or "").lower()
        recent_count = int(item.get("event_recent_count", 0) or 0)
        alignment = int(_to_float(item.get("alignment_score"), 0))
        interpretation = str(item.get("event_interpretation_label") or "해석 보류")
        confidence = str(item.get("event_backtest_confidence") or "-")
        valid_sample = int(item.get("event_valid_sample_size", 0) or 0)
        total_sample = int(item.get("event_sample_size", 0) or 0)
        reaction = str(item.get("event_reaction_profile") or "")
        action = str(item.get("event_tactical_label") or "").strip()
        take = str(item.get("event_tactical_note") or "").strip()
        if not action:
            if interpretation == "참고 가능" and bias == "positive":
                action = "참고 가능"
                take = "과거 평균과 승률, 낙폭을 함께 보면 우호적인 편입니다."
            elif interpretation == "변동성 주의":
                action = "변동성 주의"
                take = "평균 수익은 괜찮아도 중간 흔들림이 커 단독 재료로 보기엔 부담이 있습니다."
            elif interpretation == "보수적" or bias == "negative":
                action = "보수적"
                take = "과거 평균 기준으로는 조심스러운 편이라 반등 시 수급 확인이 먼저입니다."
            else:
                action = "해석 보류"
                if bias == "neutral":
                    take = "방향성이 약한 공시라 단독 판단 재료로 쓰기 어렵고, 사후 성과도 아직 충분히 쌓이지 않았습니다."
                else:
                    take = "아직 5일 사후 성과가 충분히 쌓이지 않아 단독 신호로 해석하기 어렵습니다."
        if bias == "positive" and expected < 0:
            take += " 현재 표본만 보면 기대수익이 아직 음수여서 더 보수적으로 보는 편이 낫습니다."
        if alignment <= -2:
            if action == "참고 가능":
                action = "변동성 주의"
            take += " 시장 큰 흐름과는 엇갈려 단독 재료 해석은 더 보수적으로 보는 편이 낫습니다."
        elif alignment >= 1 and bias == "positive":
            take += " 섹터 맥락도 우호적인 편이라 대표주 기준 눌림 확인 가치가 있습니다."
        sector_tactical = str(item.get("event_sector_tactical_label") or "").strip()
        sector_sample = int(item.get("event_sector_valid_sample_size", 0) or 0)
        sector_name = str(item.get("event_sector") or item.get("sector") or "").strip()
        sector_note = str(item.get("event_sector_tactical_note") or "").strip()
        sector_display = sector_tactical if sector_sample > 0 else "-"
        if sector_sample > 0 and sector_tactical and sector_tactical != action:
            take += f" 같은 `{sector_name or item.get('sector') or '-'}` 섹터만 따로 보면 `{sector_tactical}` 쪽에 가깝습니다."
            if sector_note:
                take += f" {sector_note}"
        take += " " + _event_path_take(item)
        lines.append(
            f"* {_display_label(item)} | {item['sector']} | 판단 `{action}` | 맥락 `{item.get('alignment_label') or '중립'}` | "
            f"`{item.get('event_last_type', '-')}` / `{item.get('event_last_bias', '-')}` | 해석 `{interpretation}` | "
            f"신뢰도 `{confidence}` | 반응 `{reaction or '-'}` | 유효5일 `{valid_sample}/{total_sample}` | "
            f"섹터판단 `{sector_display}` ({sector_name or item.get('sector') or '-'} {sector_sample}) | "
            f"초기 `{_fmt_pct(item.get('event_expected_alpha_1d'), 2)}` / `{_fmt_pct(item.get('event_expected_alpha_3d'), 2)}` | 기대5일 `{_fmt_pct(item.get('event_expected_alpha_5d'), 2)}`"
        )
        lines.append(f"  해석: {take}")
    return lines


def render_sector_lines(sectors: list[dict], top_n: int) -> list[str]:
    if not sectors:
        return []
    lines = ["", "*5. 섹터 체온*"]
    attack = []
    focus = []
    hold = []
    for item in sectors[: min(5, top_n)]:
        macro = _to_float(item.get("avg_macro_sector_score"))
        card = _to_float(item.get("avg_card_score"))
        leaders = ", ".join((item.get("leaders") or [])[:2]) or "-"
        count = int(item.get("count", 0) or 0)
        alignment = int(_to_float(item.get("alignment_score"), 0))
        thesis_label = str(item.get("sector_final_label") or "")
        thesis_summary = str(item.get("sector_human_summary") or "")
        if thesis_label == "공통 우호":
            action = "섹터 추적"
            take = thesis_summary or "세 렌즈가 함께 우호적인 편이라 섹터 전체를 볼 수 있습니다"
            attack.append(item)
        elif thesis_label == "단기 순환 우세":
            action = "중심주만"
            take = thesis_summary or "수급은 뜨지만 추격보다 눌림과 후발주 확인이 먼저입니다"
            focus.append(item)
        elif thesis_label == "체력 우위":
            action = "중심주만"
            take = thesis_summary or "대장주보다 아직 덜 오른 체력 종목을 함께 보는 편이 낫습니다"
            focus.append(item)
        elif thesis_label == "매크로 우호":
            action = "관찰"
            take = thesis_summary or "환경은 우호적이지만 흐름 확인 전까지는 관찰이 먼저입니다"
            hold.append(item)
        elif thesis_label == "보류" or alignment <= -2:
            action = "보류"
            take = thesis_summary or "섹터 점수는 보이지만 공통 결론은 아직 보류 쪽입니다"
            hold.append(item)
        elif macro >= 1 or alignment >= 2 or (card >= 0.66 and count >= 4):
            action = "섹터 추적"
            take = "섹터 전체를 볼 수는 있지만 실제 매매는 대표주 1~2개로 압축하는 편이 낫습니다"
            attack.append(item)
        elif card >= 0.62:
            action = "중심주만"
            take = "섹터 베팅보다 대표주 한두 개만 추려서 보는 편이 낫습니다"
            focus.append(item)
        else:
            action = "보류"
            take = "상위권이긴 하지만 섹터 전체 매수로 해석하기는 아직 이릅니다"
            hold.append(item)
        lines.append(
            f"* {item['sector']} | 판단 `{action}` | 공통결론 `{item.get('sector_final_label') or '-'}` | 맥락 `{item.get('alignment_label') or '중립'}` | 대표주 `{leaders}` | 점검평균 `{_fmt_num(item.get('avg_card_score'))}` | 종목수 `{count}`"
        )
        if item.get("sector_final_label"):
            lines.append(
                f"  세 렌즈: 수급 `{_fmt_num(item.get('sector_flow_lens_score'), 1)}` | "
                f"퀀트 `{_fmt_num(item.get('sector_quant_lens_score'), 1)}` | "
                f"매크로 `{_fmt_num(item.get('sector_macro_lens_score'), 1)}`"
            )
        lines.append(f"  해석: {take}")
    summary_lines = []
    if attack:
        summary_lines.append(f"- 지금 섹터로 바로 볼 만한 쪽은 `{_top_sector_names(attack)}` 입니다.")
    if focus:
        summary_lines.append(f"- 다만 대부분은 섹터 베팅보다 `{_top_sector_names(focus)}` 대표주 추적으로 보는 편이 낫습니다.")
    if hold:
        summary_lines.append(f"- `{_top_sector_names(hold)}` 쪽은 아직 섹터 전체 추세로 단정하기보다 관찰 단계에 가깝습니다.")
    if summary_lines:
        lines[2:2] = summary_lines
    return lines
