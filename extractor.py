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


def compute_hash(text: str) -> str:
    """
    Compute SHA-256 hash of the extracted text.
    Used for duplicate detection — if two files have the exact same text,
    they are considered identical orders. This prevents re-downloaded
    PDFs (which have different metadata/bytes) from being processed again.
    """
    sha256 = hashlib.sha256()
    sha256.update(text.encode('utf-8'))
    return sha256.hexdigest()
