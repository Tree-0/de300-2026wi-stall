from __future__ import annotations

import re
import uuid
from typing import Iterable

# Stable namespace so the same unresolved token gets the same synthetic MBID.
_SYNTH_NAMESPACE = uuid.UUID("4f7f8d9c-9f4f-4f07-8fa6-a1f6ef8f9f3a")
_MUSICBRAINZ_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    flags=re.IGNORECASE,
)

# Split tokens like:
#   "A feat. B, C & D" -> ["A", "B", "C", "D"]
#   "AwithB" is not split, but "A with B" and "Awith\u65e5\u672c\u8a9e" can be
#   split when marker words are clearly delimited from ASCII words.
# Slash handling is intentionally stricter and happens after the main regex so
# credits like "boywithuke/blackbear" split, but "AC/DC", URLs, "//", and
# tokens with nearby punctuation such as "+" or "-" do not.
_ARTIST_SPLIT_RE = re.compile(
        r"""
        \s*(?:,|;|&|\uFF06|\u3001|\u30FB|\u2022)\s*                # punctuation separators
        |\s+(?:x|\u00D7)\s+                                          # x / \u00D7 between names
        |\s*(?:\(|\[|\{)?\s*
            (?<![A-Za-z0-9])(?:
                featured\s+by(?![A-Za-z0-9])
                |featuring(?![A-Za-z0-9])
                |(?:feat|ft)\.(?=\S)                                  # allow feat./ft. with no space after dot
                |(?:feat|ft)\.?(?![A-Za-z0-9])
            )
            \s*(?:\)|\]|\})?\s*                                      # feat/ft/featuring/featured by
        |\s*(?:\(|\[|\{)?\s*
            (?<![A-Za-z0-9])(?:with|vs\.?|versus)(?![A-Za-z0-9])
            \s*(?:\)|\]|\})?\s*                                      # with/vs/versus
        """,
        flags=re.IGNORECASE | re.VERBOSE,
)
_SPACE_RE = re.compile(r"\s+")
_SLASH_PAIR_RE = re.compile(
    r"^\s*(?P<left>[^/\uFF0F]+?)\s*(?<!/)(?:/|\uFF0F)(?!/)\s*(?P<right>[^/\uFF0F]+?)\s*$"
)
_ASCII_UPPER_RE = re.compile(r"^[A-Z0-9]+$")
_TOKEN_EDGE_RE = re.compile(
        r"^[\s\(\[\{<\"'`\u300C\u300D\u300E\u300F\u3010\u3011\uFF08\uFF09\uFF3B\uFF3D\uFF5B\uFF5D]+"
        r"|[\s\)\]\}>\"'`\u300C\u300D\u300E\u300F\u3010\u3011\uFF08\uFF09\uFF3B\uFF3D\uFF5B\uFF5D]+$"
)


ArtistEntity = tuple[str, str | None, bool]


def normalize_artist_token(value: str | None) -> str:
    if not value:
        return ""
    cleaned = _SPACE_RE.sub(" ", value).strip().lower()
    return cleaned


def _is_clean_slash_side(value: str) -> bool:
    token = _SPACE_RE.sub(" ", value).strip()
    if not token:
        return False
    if not any(char.isalnum() for char in token):
        return False
    return all(char.isalnum() or char.isspace() for char in token)


def _looks_like_ascii_acronym(value: str) -> bool:
    token = _SPACE_RE.sub(" ", value).strip().replace(" ", "")
    return bool(token) and bool(_ASCII_UPPER_RE.fullmatch(token))


def split_slash_artist_credit(artist_credit: str | None) -> list[str]:
    if not artist_credit:
        return []

    match = _SLASH_PAIR_RE.match(artist_credit)
    if not match:
        return []

    left = _SPACE_RE.sub(" ", match.group("left")).strip()
    right = _SPACE_RE.sub(" ", match.group("right")).strip()

    if not _is_clean_slash_side(left) or not _is_clean_slash_side(right):
        return []

    if _looks_like_ascii_acronym(left) and _looks_like_ascii_acronym(right):
        return []

    return [left, right]


def split_artist_credit(artist_credit: str | None) -> list[str]:
    if not artist_credit:
        return []
    parts = _ARTIST_SPLIT_RE.split(artist_credit)
    tokens: list[str] = []
    for part in parts:
        slash_parts = split_slash_artist_credit(part)
        candidate_parts = slash_parts if slash_parts else [part]
        for candidate in candidate_parts:
            token = _SPACE_RE.sub(" ", candidate).strip()
            token = _TOKEN_EDGE_RE.sub("", token).strip()
            if token:
                tokens.append(token)
    return tokens


def has_artist_collab_markers(artist_credit: str | None) -> bool:
    if not artist_credit:
        return False
    return bool(_ARTIST_SPLIT_RE.search(artist_credit) or split_slash_artist_credit(artist_credit))


def synth_artist_mbid_from_token(token: str) -> str:
    return str(uuid.uuid5(_SYNTH_NAMESPACE, normalize_artist_token(token)))


def _coerce_artist_mbids(artist_mbids: object) -> list[str]:
    if artist_mbids is None:
        return []
    if isinstance(artist_mbids, str):
        artist_mbids = [artist_mbids]
    if not isinstance(artist_mbids, list):
        return []

    mbids: list[str] = []
    seen: set[str] = set()
    for mbid in artist_mbids:
        if not isinstance(mbid, str):
            continue
        mbid = mbid.strip()
        if not mbid or mbid in seen:
            continue
        seen.add(mbid)
        mbids.append(mbid)
    return mbids


def register_artist_aliases(alias_to_mbid: dict[str, str], artist_name: str | None, mbid: str) -> None:
    token = normalize_artist_token(artist_name)
    if token and token not in alias_to_mbid:
        alias_to_mbid[token] = mbid


def canonicalize_artist_entities(
    artist_name: str | None,
    artist_mbids: object,
    alias_to_mbid: dict[str, str] | None = None,
) -> tuple[list[ArtistEntity], int]:
    """
    Return canonical `(artist_mbid, artist_name, is_synthetic)` tuples.

    Rules:
    - Use provided MBIDs first.
    - Split multi-artist credits locally (`feat.`, `feat`, `featuring`, `,`, `&`).
    - For unmatched split tokens, reuse a known MBID alias when possible.
    - Otherwise mint a deterministic synthetic MBID from the token.
    """
    mbids = _coerce_artist_mbids(artist_mbids)
    tokens = split_artist_credit(artist_name)

    entities: list[ArtistEntity] = []
    synthetic_count = 0

    matched = min(len(tokens), len(mbids))
    for idx in range(matched):
        entities.append((mbids[idx], tokens[idx], False))

    if len(mbids) > matched:
        fallback_name = tokens[0] if len(tokens) == 1 else None
        for idx in range(matched, len(mbids)):
            entities.append((mbids[idx], fallback_name, False))

    if len(tokens) > matched:
        for idx in range(matched, len(tokens)):
            token = tokens[idx]
            normalized = normalize_artist_token(token)
            mapped_mbid = alias_to_mbid.get(normalized) if alias_to_mbid else None
            if mapped_mbid:
                entities.append((mapped_mbid, token, False))
                continue
            synthetic_count += 1
            entities.append((synth_artist_mbid_from_token(token), token, True))

    if not entities and mbids:
        for mbid in mbids:
            entities.append((mbid, artist_name, False))

    deduped: list[ArtistEntity] = []
    seen_mbids: set[str] = set()
    for mbid, token_name, is_synthetic in entities:
        if mbid in seen_mbids:
            continue
        seen_mbids.add(mbid)
        deduped.append((mbid, token_name, is_synthetic))

    return deduped, synthetic_count


def is_valid_mbid(value: str | None) -> bool:
    return bool(value and _MUSICBRAINZ_UUID_RE.match(value))


def build_alias_map(artist_info_rows: Iterable[tuple[str, str | None]]) -> dict[str, str]:
    alias_to_mbid: dict[str, str] = {}
    for mbid, name in artist_info_rows:
        if not isinstance(mbid, str):
            continue
        register_artist_aliases(alias_to_mbid, name, mbid)
    return alias_to_mbid
