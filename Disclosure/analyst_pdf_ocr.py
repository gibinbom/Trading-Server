from __future__ import annotations

import re
import shutil
import subprocess
from typing import Any

try:
    import pytesseract
except Exception:
    pytesseract = None

try:
    from pdf2image import convert_from_bytes
except Exception:
    convert_from_bytes = None


def _clean_text(text: str, max_chars: int = 12000) -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    return cleaned[: max(0, int(max_chars))]


def _best_lang(preferred: str = "kor+eng") -> str:
    if not shutil.which("tesseract"):
        return "eng"
    try:
        result = subprocess.run(["tesseract", "--list-langs"], check=True, capture_output=True, text=True)
        available = {line.strip() for line in result.stdout.splitlines() if line.strip() and "List of available" not in line}
    except Exception:
        return "eng"
    wanted = [part.strip() for part in str(preferred).split("+") if part.strip()]
    supported = [lang for lang in wanted if lang in available]
    return "+".join(supported) if supported else ("eng" if "eng" in available else (next(iter(available), "eng")))


def extract_pdf_text_with_ocr(pdf_bytes: bytes, max_pages: int = 5, lang: str = "kor+eng") -> dict[str, Any]:
    if not pdf_bytes:
        return {"pdf_text": "", "pdf_text_status": "ocr_missing_bytes", "pdf_text_length": 0}
    if pytesseract is None or convert_from_bytes is None or not shutil.which("tesseract"):
        return {"pdf_text": "", "pdf_text_status": "ocr_unavailable", "pdf_text_length": 0}
    try:
        images = convert_from_bytes(pdf_bytes, dpi=220, first_page=1, last_page=max(1, int(max_pages)), fmt="png")
        use_lang = _best_lang(lang)
        texts = [pytesseract.image_to_string(image, lang=use_lang, config="--psm 6") for image in images[: max(1, int(max_pages))]]
        text = _clean_text(" ".join(texts))
        return {"pdf_text": text, "pdf_text_status": f"ocr_{use_lang}", "pdf_text_length": len(text)}
    except Exception:
        return {"pdf_text": "", "pdf_text_status": "ocr_failed", "pdf_text_length": 0}
