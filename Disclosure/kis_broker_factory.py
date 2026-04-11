from __future__ import annotations

from broker_kis import KISBroker
from config import SETTINGS


def build_kis_broker_from_settings(*, is_virtual: bool | None = None, dry_run: bool | None = None) -> KISBroker:
    broker_is_virtual = bool(getattr(SETTINGS, "KIS_IS_VIRTUAL", False) if is_virtual is None else is_virtual)
    broker_dry_run = bool(getattr(SETTINGS, "DRY_RUN", False) if dry_run is None else dry_run)

    if broker_is_virtual:
        appkey = SETTINGS.KIS_VTS_APPKEY
        appsecret = SETTINGS.KIS_VTS_APPSECRET
        cano = SETTINGS.KIS_VTS_CANO
        acnt_prdt_cd = SETTINGS.KIS_VTS_ACNT_PRDT_CD
    else:
        appkey = SETTINGS.KIS_APPKEY
        appsecret = SETTINGS.KIS_APPSECRET
        cano = SETTINGS.KIS_CANO
        acnt_prdt_cd = SETTINGS.KIS_ACNT_PRDT_CD

    return KISBroker(
        appkey=appkey,
        appsecret=appsecret,
        cano=cano,
        acnt_prdt_cd=acnt_prdt_cd,
        is_virtual=broker_is_virtual,
        dry_run=broker_dry_run,
    )
