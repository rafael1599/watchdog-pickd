"""
extractor.py — PDF text extraction and hashing.

Primary:   pdfplumber (free, 100% accurate on text-native PDFs)
Fallback:  AI-based extraction (Gemini → OpenAI) for scanned/image PDFs
"""

import hashlib
import pdfplumber


def extract_text(pdf_path: str) -> str:
    """
    Extract all text from a PDF using pdfplumber.
    Returns the concatenated text of all pages.
    """
    full_text = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                full_text.append(text)

    return "\n".join(full_text)


def compute_hash(pdf_path: str) -> str:
    """
    Compute SHA-256 hash of the raw PDF file bytes.
    Used for exact duplicate detection — if two files produce the same hash,
    they are byte-for-byte identical.
    """
    sha256 = hashlib.sha256()

    with open(pdf_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)

    return sha256.hexdigest()
