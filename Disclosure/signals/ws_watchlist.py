from __future__ import annotations

import json
import logging
import os

import requests

from config import SETTINGS
from universe_data import build_watchlist_by_marcap


log = logging.getLogger("scanner")
KIS_APPROVAL_URL = (
    "https://openapivts.koreainvestment.com:29443/oauth2/Approval"
    if getattr(SETTINGS, "IS_VIRTUAL", False)
    else "https://openapi.koreainvestment.com:9443/oauth2/Approval"
)

FALLBACK_WATCH_MAP = {
    "005930": "삼성전자", "000660": "SK하이닉스", "042700": "한미반도체", "403870": "HPSP", "039030": "이수페타시스",
    "058470": "리노공업", "074600": "원익IPS", "252990": "에이피티씨", "036540": "SFA반도체", "084370": "유진테크",
    "036930": "주성엔지니어링", "108320": "LX세미콘", "036830": "솔브레인", "272210": "한화시스템", "005935": "삼성전자우",
    "240810": "원익피앤이", "095610": "테스", "222800": "심텍", "011070": "LG이노텍", "009150": "삼성전기",
    "090360": "로보티즈", "277810": "레인보우로보틱스", "050090": "에스에프에이", "079370": "제우스", "290560": "씨에스베어링",
    "035420": "NAVER", "035720": "카카오", "032640": "LG유플러스", "030200": "KT", "017670": "SK텔레콤",
    "018260": "삼성SDS", "034730": "SK", "402340": "SK스퀘어", "035760": "CJ ENM", "036570": "엔씨소프트",
    "259960": "크래프톤", "251270": "넷마블", "263750": "펄어비스", "293490": "카카오게임즈", "095660": "네오위즈",
    "105560": "KB금융", "055550": "신한지주", "086790": "하나금융지주", "316140": "우리금융지주", "138040": "메리츠금융지주",
    "006800": "미래에셋증권", "016360": "삼성증권", "005940": "NH투자증권", "039490": "키움증권", "032830": "삼성생명",
    "000810": "삼성화재", "000030": "우리은행", "024110": "기업은행", "323410": "카카오뱅크", "377300": "카카오페이",
    "003550": "LG", "000150": "두산", "004150": "한솔홀딩스", "000100": "유한양행", "028260": "삼성물산",
    "373220": "LG에너지솔루션", "006400": "삼성SDI", "051910": "LG화학", "096770": "SK이노베이션", "086520": "에코프로",
    "247540": "에코프로비엠", "003670": "포스코퓨처엠", "005490": "POSCO홀딩스", "348370": "엔켐", "010130": "고려아연",
    "009830": "한화솔루션", "066970": "엘앤에프", "011170": "롯데케미칼", "010950": "S-Oil", "004020": "현대제철",
    "001430": "금양", "278280": "천보", "001440": "대한전선", "036460": "한국가스공사",
    "012450": "한화에어로스페이스", "047810": "한국항공우주", "064350": "현대로템", "079550": "LIG넥스원", "042660": "한화오션",
    "010140": "삼성중공업", "329180": "HD현대중공업", "009540": "HD한국조선해양", "034020": "두산에너빌리티", "015760": "한국전력",
    "052690": "한전기술", "241560": "두산밥캣", "112610": "씨에스윈드", "047050": "포스코인터내셔널", "004380": "삼익THK",
    "012200": "계룡건설", "001500": "HDC현대산업개발", "000720": "현대건설", "028670": "팬오션", "011200": "HMM",
    "005380": "현대차", "000270": "기아", "012330": "현대모비스", "000120": "CJ대한통운", "005385": "현대차우",
    "005387": "현대차2우B", "018880": "한온시스템", "003620": "쌍용차", "014900": "디피씨", "267250": "HD현대",
    "207940": "삼성바이오로직스", "068270": "셀트리온", "196170": "알테오젠", "028300": "HLB", "068240": "셀트리온제약",
    "008930": "한미사이언스", "128940": "한미약품", "006280": "녹십자", "001630": "종근당", "096530": "씨젠",
    "090430": "아모레퍼시픽", "051900": "LG생활건강", "214150": "클래시스", "290650": "엘앤씨바이오", "328130": "루닛",
    "145020": "휴젤", "235980": "메드팩토", "003220": "대원제약", "241590": "화승엔터프라이즈",
    "352820": "하이브", "035900": "JYP Ent.", "041510": "에스엠", "122870": "와이지엔터테인먼트", "253450": "스튜디오드래곤",
    "033780": "KT&G", "027410": "BGF리테일", "139480": "이마트", "023530": "롯데쇼핑", "007070": "GS리테일",
    "034220": "LG디스플레이", "000880": "한화", "001040": "CJ",
}


def load_watch_map(cache_file: str = "universe_cache.json", top_n: int = 2000) -> dict[str, str]:
    try:
        watch_map = build_watchlist_by_marcap(min_marcap_krw=0, top_n=top_n, return_type="map")
        with open(cache_file, "w", encoding="utf-8") as handle:
            json.dump(watch_map, handle, ensure_ascii=False, indent=2)
        log.info("📊 유니버스 로드 완료: 총 %s개 종목 감시 시작!", len(watch_map))
        return watch_map
    except Exception as exc:
        log.error("🚨 유니버스 API 응답 오류 (점검 시간 의심): %s", exc)

    if os.path.exists(cache_file):
        log.warning("⚠️ 서버 통신 실패! 저장된 [로컬 캐시 파일]에서 유니버스를 복구합니다.")
        try:
            with open(cache_file, "r", encoding="utf-8") as handle:
                watch_map = json.load(handle)
            log.info("💾 캐시 유니버스 복구 완료: 총 %s개 종목 감시 시작!", len(watch_map))
            return watch_map
        except Exception as exc:
            log.error("캐시 로드 실패: %s", exc)

    log.warning("⚠️ 캐시 파일 없음! 최후의 보루인 [핵심 주도주 fallback]으로 강제 가동합니다.")
    return dict(FALLBACK_WATCH_MAP)


def request_approval_key() -> str:
    response = requests.post(
        KIS_APPROVAL_URL,
        json={
            "grant_type": "client_credentials",
            "appkey": SETTINGS.KIS_APPKEY,
            "secretkey": SETTINGS.KIS_APPSECRET,
        },
        timeout=10,
    )
    payload = response.json()
    if "approval_key" not in payload:
        raise RuntimeError(f"KIS 웹소켓 키 발급 실패: {payload}")
    return payload["approval_key"]
