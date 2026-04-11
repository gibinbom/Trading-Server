from __future__ import annotations


BULLISH_TERMS = {
    "kr": ["상향", "매수", "비중확대", "재평가", "본격화", "확대", "견조", "서프라이즈", "회복", "구조적"],
    "en": ["buy", "outperform", "overweight", "upgrade", "raise", "beat", "acceleration", "re-rating", "strong"],
}

BEARISH_TERMS = {
    "kr": ["하향", "중립", "축소", "부진", "둔화", "우려", "약화", "불확실", "제한적", "부담"],
    "en": ["sell", "underperform", "downgrade", "cut", "miss", "weak", "risk", "uncertain", "headwind", "concern"],
}

CONFIDENCE_TERMS = {
    "kr": ["확실", "명확", "강력", "핵심", "유효", "지속", "가속", "초입", "본격"],
    "en": ["clear", "strong", "conviction", "solid", "durable", "compelling", "inflect", "acceleration"],
}

HESITATION_TERMS = {
    "kr": ["추정", "가능성", "점검", "관망", "보수적", "확인 필요", "변수", "불확실"],
    "en": ["likely", "possible", "watch", "wait", "cautious", "uncertain", "monitor", "risk"],
}
