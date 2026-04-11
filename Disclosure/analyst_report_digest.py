from __future__ import annotations


def build_slack_digest(summary: dict, max_names: int = 10) -> str:
    lines = ["[애널리스트 리포트 요약]"]
    lines.append(f"- 생성시각: {summary.get('snapshot_at', '')}")
    lines.append("- 종목별 상위 conviction:")
    for item in summary.get("top_stocks", [])[:max_names]:
        lines.append(
            f"* {item['name']}({item['symbol']}) | {item['sector']} | conviction {item['conviction_score']} | "
            f"avg_score {item['avg_report_score']} | 목표가수정 {item['avg_target_revision_pct']}% | "
            f"novelty {item.get('avg_novelty_score', 0)} | alpha5d {item.get('avg_alpha_ret_5d', 0)}% | "
            f"upside {item.get('target_upside_pct', 0)}% | agree {item.get('agreement_score', 0)} | "
            f"리포트 {item['report_count']}건 / 증권사 {item['broker_diversity']}곳"
        )
        lines.append(f"  최근 제목: {item['latest_title']}")
        if item.get("latest_calibration_status"):
            lines.append(
                f"  보정: {item['latest_calibration_status']} | broker_adj {item.get('avg_broker_bias_adjustment', 0)}"
                f" | pdf {item.get('latest_pdf_status', '-')} | parse {item.get('latest_parse_quality_status', '-')}"
                f" | recency {item.get('recency_score', 0)} | dispersion {item.get('target_dispersion_pct', 0)}%"
            )
            lines.append(
                f"  breadth {item.get('revision_breadth_score', 0)} (net {item.get('revision_breadth_count', 0)})"
                f" | peer {item.get('peer_spillover_score', 0)} | peer_alpha5d {item.get('peer_alpha_5d', 0)}%"
                f" | peer_support {item.get('peer_support_count', 0)}"
            )
    if summary.get("top_brokers"):
        lines.append("- 최근 alpha 상위 브로커:")
        for row in summary["top_brokers"][:5]:
            lines.append(
                f"  {row['broker']} | avg_score {row['avg_report_score']} | novelty {row['avg_novelty_score']} | "
                f"alpha5d {row['avg_alpha_ret_5d']}% | roll_alpha {row.get('avg_rolling_alpha_mean', 0)} | reports {row['report_count']}"
            )
    if summary.get("top_terms"):
        lines.append("- 반복 제목 키워드: " + ", ".join(f"{row['term']} {row['count']}" for row in summary["top_terms"][:10]))
    return "\n".join(lines)
