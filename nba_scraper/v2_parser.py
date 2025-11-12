"""Parser that converts legacy stats.nba.com v2 JSON into canonical rows."""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import pandas as pd

from .mapping.descriptor_norm import normalize_descriptor
from .helper_functions import seconds_elapsed

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


def _load_dataframe(v2_json: Dict) -> pd.DataFrame:
    result_sets = v2_json.get("resultSets") or []
    if not result_sets:
        return pd.DataFrame(columns=_CANONICAL_COLUMNS)
    rs = result_sets[0]
    headers = [h.lower() for h in rs.get("headers", [])]
    rows = rs.get("rowSet", [])
    return pd.DataFrame(rows, columns=headers)


def _infer_team_meta(df: pd.DataFrame) -> Tuple[Optional[int], Optional[str], Optional[int], Optional[str]]:
    seen: List[Tuple[Optional[int], Optional[str]]] = []
    for col_id, col_abbrev in [
        ("player1_team_id", "player1_team_abbreviation"),
        ("player2_team_id", "player2_team_abbreviation"),
        ("player3_team_id", "player3_team_abbreviation"),
        ("team_id", "team_abbreviation"),
    ]:
        if col_id not in df or col_abbrev not in df:
            continue
        for tid, abbrev in zip(df[col_id], df[col_abbrev]):
            if pd.isna(tid) or pd.isna(abbrev):
                continue
            pair = (int(tid), str(abbrev))
            if pair not in seen:
                seen.append(pair)
            if len(seen) >= 2:
                break
        if len(seen) >= 2:
            break
    if len(seen) == 1:
        seen.append((None, None))
    away = seen[0] if seen else (None, None)
    home = seen[1] if len(seen) > 1 else (None, None)
    return home[0], home[1], away[0], away[1]


def _family(row: pd.Series) -> str:
    eventmsgtype = int(row.get("eventmsgtype", 0) or 0)
    if eventmsgtype in (1, 2):
        desc = "{} {}".format(row.get("homedescription", ""), row.get("visitordescription", ""))
        desc = desc.upper()
        if "3PT" in desc:
            return "3pt"
        return "2pt"
    mapping = {
        3: "freethrow",
        4: "rebound",
        5: "turnover",
        6: "foul",
        7: "violation",
        8: "substitution",
        9: "timeout",
        10: "jumpball",
        12: "period",
        13: "period",
        15: "game",
    }
    return mapping.get(eventmsgtype, "")


def _shot_made(row: pd.Series) -> Optional[int]:
    eventmsgtype = int(row.get("eventmsgtype", 0) or 0)
    desc = "{} {}".format(row.get("homedescription", ""), row.get("visitordescription", "")).upper()
    if eventmsgtype == 1:
        return 1
    if eventmsgtype == 2:
        return 0
    if eventmsgtype == 3:
        if "MISS" in desc:
            return 0
        if desc.strip():
            return 1
    return None


def _points_made(family: str, shot_made: Optional[int]) -> int:
    if shot_made != 1:
        return 0
    if family == "3pt":
        return 3
    if family == "freethrow":
        return 1
    return 2


def _subfamily(row: pd.Series, family: str) -> str:
    raw = row.get("eventmsgactiontype")
    if pd.isna(raw):
        raw = None
    descriptor = row.get("homedescription") or row.get("visitordescription")
    desc_core, _ = normalize_descriptor(descriptor)
    if family in {"turnover", "foul", "violation"} and desc_core:
        return desc_core
    if raw is None:
        return desc_core
    return str(int(raw))


def _team_id_for_row(row: pd.Series) -> Optional[int]:
    for key in ("team_id", "player1_team_id", "player2_team_id", "player3_team_id"):
        value = row.get(key)
        if not pd.isna(value):
            return int(value)
    return None


def _team_tricode_for_row(row: pd.Series) -> Optional[str]:
    for key in (
        "team_abbreviation",
        "player1_team_abbreviation",
        "player2_team_abbreviation",
        "player3_team_abbreviation",
    ):
        value = row.get(key)
        if value and isinstance(value, str):
            return value
    return None


def parse_v2_to_rows(v2_json: Dict, mapping_yaml_path: Optional[str] = None) -> pd.DataFrame:
    df_raw = _load_dataframe(v2_json)
    if df_raw.empty:
        return pd.DataFrame(columns=_CANONICAL_COLUMNS)

    home_id, home_tri, away_id, away_tri = _infer_team_meta(df_raw)

    rows: List[Dict[str, object]] = []
    for _, row in df_raw.iterrows():
        family = _family(row)
        pctimestring = row.get("pctimestring") or "12:00"
        secs = seconds_elapsed(row.get("period"), pctimestring)
        shot_made = _shot_made(row)
        points = _points_made(family, shot_made)
        subfamily = _subfamily(row, family)
        rows.append(
            {
                "game_id": row.get("game_id"),
                "period": row.get("period"),
                "pctimestring": pctimestring,
                "seconds_elapsed": secs,
                "event_length": None,
                "action_number": row.get("eventnum"),
                "order_number": row.get("eventnum"),
                "time_actual": row.get("game_clock"),
                "team_id": _team_id_for_row(row),
                "team_tricode": _team_tricode_for_row(row),
                "player1_id": row.get("player1_id"),
                "player1_name": row.get("player1_name"),
                "player2_id": row.get("player2_id"),
                "player2_name": row.get("player2_name"),
                "player3_id": row.get("player3_id"),
                "player3_name": row.get("player3_name"),
                "home_team_id": home_id,
                "home_team_abbrev": home_tri,
                "away_team_id": away_id,
                "away_team_abbrev": away_tri,
                "game_date": row.get("game_date"),
                "family": family,
                "subfamily": subfamily,
                "eventmsgtype": row.get("eventmsgtype"),
                "eventmsgactiontype": row.get("eventmsgactiontype"),
                "is_three": 1 if family == "3pt" else 0,
                "shot_made": shot_made,
                "points_made": points,
                "shot_distance": row.get("shot_distance"),
                "x": row.get("loc_x"),
                "y": row.get("loc_y"),
                "side": None,
                "area": None,
                "area_detail": None,
                "assist_id": row.get("assist_person_id"),
                "block_id": row.get("block_person_id"),
                "steal_id": row.get("steal_person_id"),
                "style_flags": [],
                "qualifiers": [],
                "is_o_rebound": 1 if family == "rebound" and "OFF" in str(row.get("homedescription", "")).upper() else 0,
                "is_d_rebound": 1 if family == "rebound" and "DEF" in str(row.get("homedescription", "")).upper() else 0,
                "team_rebound": 1 if int(row.get("player1_id", 0) or 0) == 0 else 0,
                "linked_shot_action_number": None,
                "possession_after": None,
                "score_home": row.get("score_home"),
                "score_away": row.get("score_away"),
            }
        )

    df = pd.DataFrame(rows, columns=_CANONICAL_COLUMNS)
    df = df.sort_values(["period", "seconds_elapsed", "order_number"], kind="mergesort")
    df["event_length"] = df.groupby("period")["seconds_elapsed"].diff(-1).abs()
    df["event_length"] = df["event_length"].fillna(0)
    return df.reset_index(drop=True)
