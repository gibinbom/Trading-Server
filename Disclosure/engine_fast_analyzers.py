from __future__ import annotations

import re
from typing import Optional, Tuple


def analyze_fast_performance(html: str, consensus: dict) -> Tuple[bool, bool, str, Optional[float]]:
    try:
        text = re.sub(r'<[^>]+>', ' ', html).replace("&nbsp;", " ").replace(",", "")
        op_match = re.search(r'영업이익.*?(-?\d+)', text)
        ni_match = re.search(r'당기순이익.*?(-?\d+)', text)

        op_val = float(op_match.group(1)) if op_match else 0.0
        ni_val = float(ni_match.group(1)) if ni_match else 0.0

        if op_val < 0:
            return False, False, f"OP_LOSS(적자): {op_val:,.0f}", None

        op_uk = op_val / 100.0
        ni_uk = ni_val / 100.0
        cons_op = consensus.get("operating_profit")
        cons_ni = consensus.get("net_income")

        reasons = []
        is_beat = False
        is_miss = False
        tp_hint = None

        if cons_op and cons_op > 0:
            ratio = op_uk / cons_op
            if ratio >= 1.3:
                is_beat = True
                reasons.append(f"OP_SURPRISE({ratio*100:.0f}%)")
                if ratio >= 2.0:
                    tp_hint = 8.0
            elif ratio <= 0.7:
                is_miss = True

        if cons_ni and cons_ni > 0:
            ratio = ni_uk / cons_ni
            if ratio >= 1.3:
                is_beat = True
                reasons.append(f"NI_SURPRISE({ratio*100:.0f}%)")
                if ratio >= 2.0:
                    tp_hint = 8.0

        return is_beat, is_miss, ", ".join(reasons), tp_hint
    except Exception:
        return False, False, "FAST_PARSE_ERR", None


def analyze_fast_supply_contract(html: str) -> Tuple[bool, str, float]:
    try:
        clean_text = html.replace("&nbsp;", " ").replace(",", "").replace("\xa0", " ")
        text_content = re.sub(r'<[^>]+>', ' ', clean_text)
        text_content = re.sub(r'\s+', ' ', text_content).strip()
        ratio_match = re.search(r'매출액\s*대비[^\d-]*?([\d\.]+)', text_content)
        amt_match = re.search(r'계약금액[^\d-]*?(-?\d+)', text_content)
        amt_val = float(amt_match.group(1)) if amt_match else 0.0

        if ratio_match:
            try:
                ratio = float(ratio_match.group(1))
                if ratio > 1000:
                    return False, "Ratio parse error (too big)", 0.0
                if ratio >= 8.0:
                    return True, f"SupplyContract(FAST): Ratio {ratio:.2f}% (Amt:{amt_val:,.0f})", ratio
                return False, f"Ratio too low ({ratio:.2f}%)", ratio
            except ValueError:
                pass
        return False, "Ratio not found", 0.0
    except Exception:
        return False, "FAST_PARSE_ERR", 0.0
