"""Parser that converts CDN liveData play-by-play feeds into canonical rows."""
from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from .mapping.descriptor_norm import canon_str, normalize_descriptor
from .mapping.event_codebook import (
    actiontype_code_for,
    eventmsgtype_for,
    ft_n_m,
)
from .helper_functions import get_season, iso_clock_to_pctimestring, seconds_elapsed
from .mapping.loader import load_mapping

_SYNTH_FT_DESC = os.getenv("NBA_SCRAPER_SYNTH_FT_DESC", "0") == "1"

_EVENT_TYPE_DE = {
    1: "shot",
    2: "missed_shot",
    3: "free-throw",
    4: "rebound",
    5: "turnover",
    6: "foul",
    7: "violation",
    8: "substitution",
    9: "timeout",
    10: "jump-ball",
    12: "period",
    13: "period",
    15: "game",
}


def _int_or_zero(value: Any) -> int:
    try:
        if value in (None, ""):
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0

_CANONICAL_COLUMNS = [
    "game_id",
    "period",
    "pctimestring",
    "seconds_elapsed",
    "event_length",
    "action_number",
    "order_number",
    "eventnum",
    "time_actual",
    "team_id",
    "team_tricode",
    "event_team",
    "player1_id",
    "player1_name",
    "player1_team_id",
    "player2_id",
    "player2_name",
    "player2_team_id",
    "player3_id",
    "player3_name",
    "player3_team_id",
    "home_team_id",
    "home_team_abbrev",
    "away_team_id",
    "away_team_abbrev",
    "homedescription",
    "visitordescription",
    "game_date",
    "season",
    "family",
    "subfamily",
    "eventmsgtype",
    "eventmsgactiontype",
    "event_type_de",
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
    "scoremargin",
    "is_turnover",
    "is_steal",
    "is_block",
    "ft_n",
    "ft_m",
]


def _opponent_team_id(
    acting_team_id: int, home_team_id: Optional[int], away_team_id: Optional[int]
) -> int:
    if acting_team_id and home_team_id and acting_team_id == int(home_team_id):
        return _int_or_zero(away_team_id)
    if acting_team_id and away_team_id and acting_team_id == int(away_team_id):
        return _int_or_zero(home_team_id)
    return 0


class _SidecarCollector:
    def __init__(self) -> None:
        self._by_shot: Dict[Tuple[int, Optional[int]], Dict[str, Any]] = {}
        self._pending_rows: Dict[Tuple[int, Optional[int]], List[Dict[str, Any]]] = {}

    def register(self, action: Dict[str, Any]) -> None:
        key = (action.get("period"), action.get("shotActionNumber"))
        entry = self._by_shot.setdefault(key, {})
        if action.get("actionType") == "block":
            entry["block_id"] = action.get("personId")
            entry["block_name"] = action.get("playerName")
        if action.get("actionType") == "steal":
            entry["steal_id"] = action.get("personId")
            entry["steal_name"] = action.get("playerName")
        if key in self._pending_rows:
            for row in self._pending_rows.pop(key):
                self._apply_entry(entry, row)

    def apply(self, action: Dict[str, Any], row: Dict[str, Any]) -> None:
        key = (action.get("period"), action.get("shotActionNumber"))
        data = self._by_shot.get(key)
        if not data:
            self._pending_rows.setdefault(key, []).append(row)
            return
        self._apply_entry(data, row)

    def _apply_entry(self, data: Dict[str, Any], row: Dict[str, Any]) -> None:
        if data.get("block_id"):
            row["block_id"] = data["block_id"]
            if data.get("block_name"):
                row["player3_name"] = data["block_name"]
            if not _int_or_zero(row.get("player3_team_id")):
                row["player3_team_id"] = _opponent_team_id(
                    _int_or_zero(row.get("player1_team_id")),
                    row.get("home_team_id"),
                    row.get("away_team_id"),
                )
            if not _int_or_zero(row.get("player3_id")):
                row["player3_id"] = data["block_id"]
            if row.get("family") in {"2pt", "3pt"} and row.get("shot_made") == 0:
                row["is_block"] = 1
        if data.get("steal_id"):
            row["steal_id"] = data["steal_id"]
            if data.get("steal_name"):
                row["player2_name"] = data["steal_name"]
            if row.get("family") == "turnover":
                row["player2_id"] = data["steal_id"]
                if not _int_or_zero(row.get("player2_team_id")):
                    row["player2_team_id"] = _opponent_team_id(
                        _int_or_zero(row.get("player1_team_id")),
                        row.get("home_team_id"),
                        row.get("away_team_id"),
                    )
                row["is_steal"] = 1


def _qualifiers_list(action: Dict[str, Any]) -> List[str]:
    quals = action.get("qualifiers") or []
    normalized = {canon_str(q) for q in quals if q}
    return sorted(q for q in normalized if q)


def _scoremargin_str(score_home: Optional[int], score_away: Optional[int]) -> str:
    try:
        if score_home is None or score_away is None:
            return ""
        return str(int(score_home) - int(score_away))
    except Exception:
        return ""


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
    game_timestamp = pd.to_datetime(game_date, utc=True, errors="coerce")
    season_val: Optional[int] = None
    if game_timestamp is not None and not pd.isna(game_timestamp):
        ts = game_timestamp
        if ts.tzinfo is not None:
            ts = ts.tz_convert(None)
        season_val = get_season(ts.to_pydatetime())

    mapping = load_mapping(mapping_yaml_path)

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
        subfamily_raw = action.get("subType") or descriptor_core
        subfamily_norm = canon_str(subfamily_raw)
        subfamily = subfamily_norm or subfamily_raw
        shot_result = action.get("shotResult")
        eventmsgtype = eventmsgtype_for(family, shot_result, subfamily)
        eventmsgactiontype = actiontype_code_for(family, subfamily)
        shot_made = _shot_made_value(family, shot_result)
        points = _points_made(family, shot_made)
        is_three = 1 if family == "3pt" else 0
        score_home, score_away = _score_tuple(action)

        team_id_raw = action.get("teamId")
        team_id_int = _int_or_zero(team_id_raw)
        event_team = action.get("teamTricode") or ""
        opp_team_id = _opponent_team_id(team_id_int, home_id, away_id)

        qualifiers_list = _qualifiers_list(action)

        sig_key = (
            canon_str(family),
            canon_str(subfamily),
            canon_str(descriptor_core),
            tuple(sorted(canon_str(q) for q in qualifiers_list)),
        )
        overrides = mapping.get(sig_key)

        ft_n_val: Optional[int] = None
        ft_m_val: Optional[int] = None
        if family == "freethrow":
            ft_n_val, ft_m_val = ft_n_m(action.get("subType") or "")
            if ft_n_val is None or ft_m_val is None:
                ft_n_val = 1
                ft_m_val = 1

        row: Dict[str, Any] = {
            "game_id": game_id,
            "period": action.get("period"),
            "pctimestring": pctimestring,
            "seconds_elapsed": secs,
            "event_length": None,
            "action_number": action.get("actionNumber"),
            "order_number": action.get("orderNumber"),
            "eventnum": action.get("actionNumber"),
            "time_actual": action.get("timeActual"),
            "team_id": action.get("teamId"),
            "team_tricode": action.get("teamTricode"),
            "event_team": event_team,
            "player1_id": action.get("personId"),
            "player1_name": action.get("playerName"),
            "player1_team_id": team_id_int,
            "player2_id": action.get("secondaryPersonId"),
            "player2_name": action.get("secondaryPlayerName"),
            "player2_team_id": 0,
            "player3_id": action.get("tertiaryPersonId"),
            "player3_name": action.get("tertiaryPlayerName"),
            "player3_team_id": 0,
            "home_team_id": home_id,
            "home_team_abbrev": home_tri,
            "away_team_id": away_id,
            "away_team_abbrev": away_tri,
            "homedescription": "",
            "visitordescription": "",
            "game_date": game_timestamp,
            "season": season_val if season_val is not None else 0,
            "family": family,
            "subfamily": subfamily,
            "eventmsgtype": eventmsgtype,
            "eventmsgactiontype": eventmsgactiontype,
            "event_type_de": _EVENT_TYPE_DE.get(eventmsgtype, ""),
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
            "qualifiers": qualifiers_list,
            "is_o_rebound": 1 if family == "rebound" and subfamily == "offensive" else 0,
            "is_d_rebound": 1 if family == "rebound" and subfamily == "defensive" else 0,
            "team_rebound": 1 if (action.get("personId") in (0, None)) else 0,
            "linked_shot_action_number": action.get("shotActionNumber"),
            "possession_after": action.get("possession"),
            "score_home": score_home,
            "score_away": score_away,
            "scoremargin": _scoremargin_str(score_home, score_away),
            "is_turnover": 1 if family == "turnover" else 0,
            "is_steal": 0,
            "is_block": 0,
            "ft_n": ft_n_val,
            "ft_m": ft_m_val,
        }

        if overrides:
            if family not in {"2pt", "3pt"} and "eventmsgtype" in overrides:
                row["eventmsgtype"] = int(overrides["eventmsgtype"])
            if "eventmsgactiontype" in overrides:
                row["eventmsgactiontype"] = int(overrides["eventmsgactiontype"])
            if overrides.get("subfamily"):
                row["subfamily"] = str(overrides["subfamily"])
            row["event_type_de"] = _EVENT_TYPE_DE.get(row["eventmsgtype"], "")

        sidecars.apply(action, row)

        row["assist_id"] = _int_or_zero(row.get("assist_id"))
        row["block_id"] = _int_or_zero(row.get("block_id"))
        row["steal_id"] = _int_or_zero(row.get("steal_id"))

        if family in {"2pt", "3pt"} and shot_made == 1:
            assist_id = row.get("assist_id")
            if assist_id:
                row["player2_id"] = assist_id
                row["player2_team_id"] = team_id_int
        if family == "turnover":
            steal_id = row.get("steal_id")
            if steal_id:
                row["player2_id"] = steal_id
                row["player2_team_id"] = opp_team_id
        if family in {"2pt", "3pt"} and shot_made == 0:
            block_id = row.get("block_id")
            if block_id:
                row["player3_id"] = block_id
                row["player3_team_id"] = opp_team_id

        row["player1_team_id"] = _int_or_zero(row.get("player1_team_id"))
        row["player2_id"] = _int_or_zero(row.get("player2_id"))
        row["player3_id"] = _int_or_zero(row.get("player3_id"))
        row["player2_team_id"] = _int_or_zero(row.get("player2_team_id"))
        row["player3_team_id"] = _int_or_zero(row.get("player3_team_id"))
        eventmsgtype_final = _int_or_zero(row.get("eventmsgtype"))
        row["event_type_de"] = _EVENT_TYPE_DE.get(eventmsgtype_final, "")
        row["is_turnover"] = 1 if eventmsgtype_final == 5 else 0
        row["is_steal"] = (
            1 if row["is_turnover"] and _int_or_zero(row.get("steal_id")) else 0
        )
        row["is_block"] = (
            1
            if family in {"2pt", "3pt"}
            and shot_made == 0
            and _int_or_zero(row.get("block_id"))
            else 0
        )

        if (
            _SYNTH_FT_DESC
            and family == "freethrow"
            and ft_n_val is not None
            and ft_m_val is not None
        ):
            if row.get("team_id") == home_id:
                row["homedescription"] = f"Free Throw {ft_n_val} of {ft_m_val}"
                row["visitordescription"] = ""
            elif row.get("team_id") == away_id:
                row["visitordescription"] = f"Free Throw {ft_n_val} of {ft_m_val}"
                row["homedescription"] = ""

        rows.append(row)

    df = pd.DataFrame(rows, columns=_CANONICAL_COLUMNS)
    if df.empty:
        return df

    df = df.sort_values(["period", "order_number", "action_number"], kind="mergesort")
    df["event_length"] = df.groupby("period")["seconds_elapsed"].diff(-1).abs()
    df["event_length"] = df["event_length"].fillna(0)
    return df.reset_index(drop=True)
