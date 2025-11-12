"""Parser that converts CDN liveData play-by-play feeds into canonical rows."""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from .mapping.descriptor_norm import normalize_descriptor
from .mapping.event_codebook import actiontype_code_for, eventmsgtype_for
from .helper_functions import iso_clock_to_pctimestring, seconds_elapsed

_CANONICAL_COLUMNS = [
    "game_id",
    "period",
    "pctimestring",
    "seconds_elapsed",
    "event_length",
    "action_number",
    "order_number",
    "time_actual",
    "team_id",
    "team_tricode",
    "player1_id",
    "player1_name",
    "player2_id",
    "player2_name",
    "player3_id",
    "player3_name",
    "home_team_id",
    "home_team_abbrev",
    "away_team_id",
    "away_team_abbrev",
    "game_date",
    "family",
    "subfamily",
    "eventmsgtype",
    "eventmsgactiontype",
    "is_three",
    "shot_made",
    "points_made",
    "shot_distance",
    "x",
    "y",
    "side",
    "area",
    "area_detail",
    "assist_id",
    "block_id",
    "steal_id",
    "style_flags",
    "qualifiers",
    "is_o_rebound",
    "is_d_rebound",
    "team_rebound",
    "linked_shot_action_number",
    "possession_after",
    "score_home",
    "score_away",
]


class _SidecarCollector:
    def __init__(self) -> None:
        self._by_shot: Dict[Tuple[int, Optional[int]], Dict[str, Any]] = {}

    def register(self, action: Dict[str, Any]) -> None:
        key = (action.get("period"), action.get("shotActionNumber"))
        entry = self._by_shot.setdefault(key, {})
        if action.get("actionType") == "block":
            entry["block_id"] = action.get("personId")
            entry["block_name"] = action.get("playerName")
        if action.get("actionType") == "steal":
            entry["steal_id"] = action.get("personId")
            entry["steal_name"] = action.get("playerName")

    def apply(self, action: Dict[str, Any], row: Dict[str, Any]) -> None:
        key = (action.get("period"), action.get("shotActionNumber"))
        data = self._by_shot.get(key)
        if not data:
            return
        if data.get("block_id"):
            row["block_id"] = data["block_id"]
        if data.get("steal_id"):
            row["steal_id"] = data["steal_id"]


def _qualifiers_list(action: Dict[str, Any]) -> List[str]:
    quals = action.get("qualifiers") or []
    return sorted({q.lower() for q in quals})


def _family_from_action(action: Dict[str, Any]) -> str:
    fam = (action.get("actionType") or "").lower()
    if fam in {"made shot", "made"}:
        fam = "2pt"
    return fam


def _shot_made_value(family: str, shot_result: Optional[str]) -> Optional[int]:
    if family not in {"2pt", "3pt", "freethrow"}:
        return None
    if shot_result is None:
        return None
    if shot_result.lower() == "made":
        return 1
    if shot_result.lower() == "missed":
        return 0
    return None


def _points_made(family: str, shot_made: Optional[int]) -> int:
    if shot_made != 1:
        return 0
    if family == "3pt":
        return 3
    if family == "freethrow":
        return 1
    return 2


def _score_tuple(action: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    score = action.get("score") or {}
    return score.get("home"), score.get("away")


def _team_meta(box_json: Dict[str, Any]) -> Tuple[int, str, int, str, str]:
    game_meta = box_json.get("game", {})
    home = game_meta.get("homeTeam", {})
    away = game_meta.get("awayTeam", {})
    home_id = home.get("teamId")
    away_id = away.get("teamId")
    home_tri = home.get("teamTricode")
    away_tri = away.get("teamTricode")
    game_date = game_meta.get("gameTimeUTC")
    return home_id, home_tri, away_id, away_tri, game_date


def parse_actions_to_rows(
    pbp_json: Dict[str, Any],
    box_json: Dict[str, Any],
    mapping_yaml_path: Optional[str] = None,
) -> pd.DataFrame:
    actions = pbp_json.get("game", {}).get("actions", [])
    game_id = pbp_json.get("game", {}).get("gameId")
    home_id, home_tri, away_id, away_tri, game_date = _team_meta(box_json)

    rows: List[Dict[str, Any]] = []
    sidecars = _SidecarCollector()

    for action in actions:
        family = _family_from_action(action)
        if family in {"block", "steal"}:
            sidecars.register(action)
            continue

        pctimestring = iso_clock_to_pctimestring(action.get("clock"))
        secs = seconds_elapsed(action.get("period"), pctimestring)

        descriptor_core, style_flags = normalize_descriptor(action.get("descriptor"))
        subfamily = action.get("subType") or descriptor_core
        shot_result = action.get("shotResult")
        eventmsgtype = eventmsgtype_for(family, shot_result, subfamily)
        eventmsgactiontype = actiontype_code_for(family, subfamily)
        shot_made = _shot_made_value(family, shot_result)
        points = _points_made(family, shot_made)
        is_three = 1 if family == "3pt" else 0
        score_home, score_away = _score_tuple(action)

        row: Dict[str, Any] = {
            "game_id": game_id,
            "period": action.get("period"),
            "pctimestring": pctimestring,
            "seconds_elapsed": secs,
            "event_length": None,
            "action_number": action.get("actionNumber"),
            "order_number": action.get("orderNumber"),
            "time_actual": action.get("timeActual"),
            "team_id": action.get("teamId"),
            "team_tricode": action.get("teamTricode"),
            "player1_id": action.get("personId"),
            "player1_name": action.get("playerName"),
            "player2_id": action.get("secondaryPersonId"),
            "player2_name": action.get("secondaryPlayerName"),
            "player3_id": action.get("tertiaryPersonId"),
            "player3_name": action.get("tertiaryPlayerName"),
            "home_team_id": home_id,
            "home_team_abbrev": home_tri,
            "away_team_id": away_id,
            "away_team_abbrev": away_tri,
            "game_date": game_date,
            "family": family,
            "subfamily": subfamily,
            "eventmsgtype": eventmsgtype,
            "eventmsgactiontype": eventmsgactiontype,
            "is_three": is_three,
            "shot_made": shot_made,
            "points_made": points,
            "shot_distance": action.get("shotDistance"),
            "x": action.get("x"),
            "y": action.get("y"),
            "side": action.get("side"),
            "area": action.get("area"),
            "area_detail": action.get("areaDetail"),
            "assist_id": action.get("assistPersonId"),
            "block_id": None,
            "steal_id": action.get("stealPersonId"),
            "style_flags": style_flags,
            "qualifiers": _qualifiers_list(action),
            "is_o_rebound": 1 if family == "rebound" and subfamily == "offensive" else 0,
            "is_d_rebound": 1 if family == "rebound" and subfamily == "defensive" else 0,
            "team_rebound": 1 if (action.get("personId") in (0, None)) else 0,
            "linked_shot_action_number": action.get("shotActionNumber"),
            "possession_after": action.get("possession"),
            "score_home": score_home,
            "score_away": score_away,
        }
        sidecars.apply(action, row)
        rows.append(row)

    df = pd.DataFrame(rows, columns=_CANONICAL_COLUMNS)
    if df.empty:
        return df

    df = df.sort_values(["period", "order_number", "action_number"], kind="mergesort")
    df["event_length"] = df.groupby("period")["seconds_elapsed"].diff(-1).abs()
    df["event_length"] = df["event_length"].fillna(0)
    return df.reset_index(drop=True)
