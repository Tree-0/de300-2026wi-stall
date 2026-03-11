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

# Split tokens like: "A feat. B, C & D" -> ["A", "B", "C", "D"]
_ARTIST_SPLIT_RE = re.compile(
    r"\s+(?:feat\.?|featuring|ft\.?)\s+|\s*&\s*|\s*,\s*|\s*;\s*|\s*•\s*",
    flags=re.IGNORECASE,
)
_SPACE_RE = re.compile(r"\s+")


ArtistEntity = tuple[str, str | None, bool]


def normalize_artist_token(value: str | None) -> str:
    if not value:
        return ""
    cleaned = _SPACE_RE.sub(" ", value).strip().lower()
    return cleaned


def split_artist_credit(artist_credit: str | None) -> list[str]:
    if not artist_credit:
        return []
    parts = _ARTIST_SPLIT_RE.split(artist_credit)
    tokens: list[str] = []
    for part in parts:
        token = _SPACE_RE.sub(" ", part).strip()
        if token:
            tokens.append(token)
    return tokens


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
