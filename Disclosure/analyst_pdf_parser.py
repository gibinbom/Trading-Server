from __future__ import annotations

import hashlib
import io
import os
import re
import shutil
import subprocess
import tempfile
from typing import Any

import pandas as pd
import requests

try:
    from analyst_pdf_ocr import extract_pdf_text_with_ocr
except Exception:
    from Disclosure.analyst_pdf_ocr import extract_pdf_text_with_ocr

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None

try:
    import pdfplumber
except Exception:
    pdfplumber = None


ROOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "analyst_reports")
PDF_CACHE_DIR = os.path.join(ROOT_DIR, "pdf_cache")


def _clean_text(text: str, max_chars: int = 12000) -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    return cleaned[: max(0, int(max_chars))]


def _cache_path(pdf_url: str) -> str:
    digest = hashlib.sha1(str(pdf_url or "").encode("utf-8")).hexdigest()
    os.makedirs(PDF_CACHE_DIR, exist_ok=True)
    return os.path.join(PDF_CACHE_DIR, f"{digest}.txt")


def _needs_ocr(text: str) -> bool:
    cleaned = _clean_text(text)
    if len(cleaned) < 80:
        return True
    tokens = re.findall(r"[0-9A-Za-z가-힣]{2,}", cleaned)
    return len(tokens) < 20


def _extract_with_pypdf(pdf_bytes: bytes, max_pages: int = 8) -> str:
    if PdfReader is None:
        return ""
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        texts = []
        for page in reader.pages[: max(1, int(max_pages))]:
            texts.append(page.extract_text() or "")
        return _clean_text(" ".join(texts))
    except Exception:
        return ""


def _extract_with_pdfplumber(pdf_bytes: bytes, max_pages: int = 8) -> str:
    if pdfplumber is None:
        return ""
    try:
        texts = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages[: max(1, int(max_pages))]:
                texts.append(page.extract_text() or "")
        return _clean_text(" ".join(texts))
    except Exception:
        return ""


def _extract_with_pdftotext(pdf_bytes: bytes) -> str:
    if not shutil.which("pdftotext"):
        return ""
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            pdf_path = os.path.join(tmpdir, "report.pdf")
            txt_path = os.path.join(tmpdir, "report.txt")
            with open(pdf_path, "wb") as fp:
                fp.write(pdf_bytes)
            subprocess.run(["pdftotext", "-layout", pdf_path, txt_path], check=True, capture_output=True)
            with open(txt_path, "r", encoding="utf-8", errors="ignore") as fp:
                return _clean_text(fp.read())
    except Exception:
        return ""


def extract_pdf_text(pdf_url: str, session: requests.Session | None = None, max_pages: int = 8, refresh: bool = False) -> dict[str, Any]:
    url = str(pdf_url or "").strip()
    if not url:
        return {"pdf_text": "", "pdf_text_status": "missing_url", "pdf_text_length": 0}
    path = _cache_path(url)
    if os.path.exists(path) and not refresh:
        try:
            with open(path, "r", encoding="utf-8") as fp:
                text = fp.read()
            return {"pdf_text": text, "pdf_text_status": "cache", "pdf_text_length": len(text)}
        except Exception:
            pass

    sess = session or requests.Session()
    sess.headers.update({"User-Agent": "Mozilla/5.0"})
    try:
        resp = sess.get(url, timeout=20)
        resp.raise_for_status()
        pdf_bytes = resp.content
    except Exception:
        return {"pdf_text": "", "pdf_text_status": "download_failed", "pdf_text_length": 0}

    text = _extract_with_pypdf(pdf_bytes, max_pages=max_pages)
    status = "pypdf"
    if not text:
        text = _extract_with_pdfplumber(pdf_bytes, max_pages=max_pages)
        status = "pdfplumber"
    if not text:
        text = _extract_with_pdftotext(pdf_bytes)
        status = "pdftotext"
    if _needs_ocr(text):
        ocr_payload = extract_pdf_text_with_ocr(pdf_bytes, max_pages=max_pages)
        if ocr_payload["pdf_text_length"] > len(text):
            text = ocr_payload["pdf_text"]
            status = ocr_payload["pdf_text_status"]
    if not text:
        status = "extract_failed"

    try:
        if text:
            with open(path, "w", encoding="utf-8") as fp:
                fp.write(text)
    except Exception:
        pass
    return {"pdf_text": text, "pdf_text_status": status, "pdf_text_length": len(text)}


def enrich_reports_with_pdf_text(df: pd.DataFrame, min_content_chars: int = 180) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    if "pdf_url" not in out.columns:
        out["pdf_url"] = ""
    if "content" not in out.columns:
        out["content"] = ""
    if "pdf_text" not in out.columns:
        out["pdf_text"] = ""
    if "pdf_text_status" not in out.columns:
        out["pdf_text_status"] = "not_requested"
    if "pdf_text_length" not in out.columns:
        out["pdf_text_length"] = 0

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    for idx, row in out.iterrows():
        current_content = str(row.get("content") or "")
        pdf_url = str(row.get("pdf_url") or "").strip()
        if len(current_content) >= int(min_content_chars) or not pdf_url:
            out.at[idx, "pdf_text"] = str(row.get("pdf_text") or "")
            out.at[idx, "pdf_text_status"] = "skipped"
            out.at[idx, "pdf_text_length"] = len(str(row.get("pdf_text") or ""))
            continue
        payload = extract_pdf_text(pdf_url, session=session)
        out.at[idx, "pdf_text"] = payload["pdf_text"]
        out.at[idx, "pdf_text_status"] = payload["pdf_text_status"]
        out.at[idx, "pdf_text_length"] = payload["pdf_text_length"]

    out["content_full"] = (
        out["content"].fillna("").astype(str).str.strip() + " " + out["pdf_text"].fillna("").astype(str).str.strip()
    ).str.strip()
    out["content_full"] = out["content_full"].where(out["content_full"].astype(str) != "", out["content"].fillna("").astype(str))
    return out
