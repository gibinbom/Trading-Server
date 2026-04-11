from __future__ import annotations


def build_report_prompt(
    report_type: str,
    covered_range: str,
    snapshot_summary: str,
    structured_summary: str,
    legacy_log_text: str,
) -> str:
    if report_type == "closing":
        prompt_instruction = """
        [특명] 당신은 15시 20분/ 19시 40분 장 마감 직전 '종가 베팅(Overnight)' 픽을 골라내는 수석 퀀트 트레이더입니다.
        [타겟 고객의 수요 & 분석 절대 원칙 🚨]
        - 고객은 대형주의 뻔한 거래대금보다 '은밀한 매집'을 원합니다.
        - 거래량이 적더라도 며칠 연속 주가를 관리하는 종목을 우선 찾으세요.
        [정량적 & 정성적 분석 기준]
        1. 최근 3일간 🛡️[방어], 🛑[투매소화], 🎯[손바뀜] 태그가 많이 누적된 종목을 우선 카운트하세요.
        2. 개인 매도 물량을 특정 창구가 가격을 크게 흔들지 않고 밑에서 계속 받아내는지 보세요.
        3. 구조화 요약의 창구 집중과 주요 가격대를 반드시 활용하세요.
        4. 단순 호가폭격(F)이나 이미 급등해버린 종목은 추천에서 엄격히 배제하세요.
        최근 3일 치 로그를 심층 분석하여, 오늘 종가에 무조건 매수해야 할 Top 2 종목을 뽑아주세요.
        단, 출력은 반드시 `관측 사실 / 추정 / 미확인` 3단 구조로 나누세요.
        숫자와 태그를 길게 늘어놓지 말고, 왜 이 두 종목이 남았는지 문장으로 설명하세요.
        """
    else:
        prompt_instruction = """
        [임무] 당신은 여의도 최상위 프랍 트레이딩 데스크의 '매집 탐지 전문가'입니다.
        [타겟 고객의 수요 & 분석 절대 원칙 🚨]
        - 거래대금 큰 종목을 나열하는 것이 아니라 박스권에서 조용히 이루어지는 '세력의 스윙 매집 타점'을 선점해야 합니다.
        - 대형주 노이즈를 걷어내고, 특정 주체가 작정하고 물량을 모으는 종목에 집중하세요.
        [정량적 & 정성적 판별 기준]
        1. 하락할 때마다 [D:방어]가 반복되는지
        2. [E:투매소화]로 개미 물량을 흡수하는지
        3. [C:쌍끌이]가 다일 누적되는지
        4. 특정 창구가 비슷한 가격대에서 반복 등장하는지
        5. [D:공격], [F:호가폭격]만 난무한 종목은 제외
        [출력 형식]
        🥷 *[세력 매집 레이더 가동 결과]*
        *0. 한 줄 요약*
        - 오늘 로그의 핵심 흐름을 한 문장으로 정리
        *1. 관측 사실*
        - 최상위 매집 의심 종목 2~3개와 구조적 근거
        *2. 추정*
        - 왜 이 흐름을 매집/분산으로 해석하는지
        - 오늘은 추격보다 눌림이 나은지, 아니면 종가 베팅이 나은지도 짧게 적기
        *3. 미확인/주의*
        - 데이터 한계, 과열 가능성, 노이즈 가능성
        [추가 지시]
        - 종목 이름을 많이 나열하지 마세요.
        - 태그 설명보다 해석 문장을 우선하세요.
        - 확신이 약하면 '오늘은 깨끗한 매집 후보가 많지 않다'고 분명히 말하세요.
        """

    context_blocks = []
    if covered_range:
        context_blocks.append(f"[로그 커버리지]\n- 분석 기간: {covered_range}")
    if snapshot_summary:
        context_blocks.append(snapshot_summary)
    if structured_summary:
        context_blocks.append(structured_summary)
    if legacy_log_text.strip():
        context_blocks.append(f"[원문 로그 일부]\n{legacy_log_text[-150000:]}")

    return f"""
    {prompt_instruction}

    [로그 데이터]
    {"\n\n".join(context_blocks)}
    """
