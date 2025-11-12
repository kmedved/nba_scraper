"""Event classification helpers shared by CDN and legacy parsers."""
from __future__ import annotations

from typing import Optional, Tuple

MSGTYPE_BY_FAMILY = {
    "freethrow": 3,
    "rebound": 4,
    "turnover": 5,
    "foul": 6,
    "violation": 7,
    "substitution": 8,
    "timeout": 9,
    "jumpball": 10,
    "period": 12,
    "game": 15,
}

TO_CODES = {
    "bad pass": 1,
    "lost ball": 2,
    "out of bounds": 3,
    "traveling": 4,
    "shot clock": 5,
    "backcourt": 6,
    "offensive goaltending": 7,
    "offensive foul": 8,
    "illegal assist": 9,
    "excess timeout": 10,
}

FOUL_CODES = {
    "personal": 1,
    "shooting": 2,
    "loose ball": 3,
    "offensive": 4,
    "away from play": 6,
    "clear path": 9,
    "technical": 11,
    "double technical": 16,
    "take": 19,
    "charge": 26,
    "defensive 3 seconds": 17,
    "flagrant 1": 14,
    "flagrant 2": 15,
}

VIOL_CODES = {
    "kicked ball": 1,
    "delay of game": 2,
    "defensive goaltending": 3,
}


def eventmsgtype_for(family: str, shot_result: Optional[str], subtype: str) -> int:
    family = family or ""
    shot_result = (shot_result or "").lower()
    subtype = subtype or ""

    if family in {"2pt", "3pt"}:
        made = shot_result == "made"
        return 1 if made else 2
    if family == "period":
        if subtype == "end":
            return 13
        return 12
    if family == "game":
        return 15
    return MSGTYPE_BY_FAMILY.get(family, 0)


def actiontype_code_for(family: str, subfamily: str) -> int:
    family = family or ""
    subfamily = (subfamily or "").lower()
    if family == "turnover":
        return TO_CODES.get(subfamily, 0)
    if family == "foul":
        return FOUL_CODES.get(subfamily, 0)
    if family == "violation":
        return VIOL_CODES.get(subfamily, 0)
    return 0


def ft_n_m(subtype: str) -> Tuple[Optional[int], Optional[int]]:
    if not subtype:
        return None, None
    parts = subtype.lower().split(" of ")
    if len(parts) != 2:
        return None, None
    try:
        n = int(parts[0].split()[-1])
        m = int(parts[1].split()[0])
    except (ValueError, IndexError):
        return None, None
    return n, m
