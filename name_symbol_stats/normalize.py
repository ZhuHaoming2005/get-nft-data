from __future__ import annotations

import re
import unicodedata

_TRAILING_ID_PATTERNS = [
    re.compile(r"\s*#\s*[0-9a-fA-FxX]+\s*$"),
    re.compile(r"\s*#\s*\d+\s*$"),
    re.compile(r"\s*-\s*\d+\s*$"),
    re.compile(r"\s*:\s*\d+\s*$"),
    re.compile(r"\s*\(\s*\d+\s*\)\s*$"),
    re.compile(r"\s*\[\s*\d+\s*\]\s*$"),
    re.compile(r"\s*/\s*\d+\s*$"),
    re.compile(r"\s+No\.?\s*\d+\s*$", re.I),
    re.compile(r"\s+nr\.?\s*\d+\s*$", re.I),
    re.compile(r"\s+\d{1,12}\s*$"),
]
_TOKEN_RE = re.compile(r"[0-9a-z]+")


def _normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def strip_trailing_number_suffix(raw: str) -> str:
    value = unicodedata.normalize("NFKC", (raw or "").strip())
    if not value:
        return ""

    changed = True
    guard = 0
    while changed and guard < 20:
        guard += 1
        changed = False
        for pattern in _TRAILING_ID_PATTERNS:
            candidate = pattern.sub("", value).strip()
            if candidate != value:
                value = candidate
                changed = True
                break
    return _normalize_spaces(value)


def normalize_name(raw: str | None) -> str:
    value = strip_trailing_number_suffix(raw or "")
    if not value:
        return ""
    value = unicodedata.normalize("NFKC", value)
    value = value.casefold()
    value = re.sub(r"[_/|:+-]+", " ", value)
    return _normalize_spaces(value)


def normalize_symbol(raw: str | None) -> str:
    value = unicodedata.normalize("NFKC", (raw or "").strip()).casefold()
    value = re.sub(r"\s+", "", value)
    return value


def tokenize_name(name_norm: str) -> tuple[str, ...]:
    return tuple(_TOKEN_RE.findall(name_norm))


def name_length_bucket(length: int, *, bucket_size: int = 4) -> int:
    if length <= 0:
        return 0
    return (length // bucket_size) * bucket_size


def build_name_signature(name_norm: str, *, limit: int = 3) -> str:
    tokens = sorted(dict.fromkeys(tokenize_name(name_norm)))
    return "+".join(tokens[:limit])


def build_name_block_key(name_norm: str) -> str:
    tokens = tokenize_name(name_norm)
    if not tokens:
        return ""
    return f"{tokens[0]}|{name_length_bucket(len(name_norm))}"
