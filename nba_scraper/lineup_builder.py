"""Utilities for attaching on-court lineups to the canonical dataframe."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

_LINEUP_ID_COLUMNS = [
    "home_player_1_id",
    "home_player_2_id",
    "home_player_3_id",
    "home_player_4_id",
    "home_player_5_id",
    "away_player_1_id",
    "away_player_2_id",
    "away_player_3_id",
    "away_player_4_id",
    "away_player_5_id",
]

_LINEUP_NAME_COLUMNS = [
    "home_player_1",
    "home_player_2",
    "home_player_3",
    "home_player_4",
    "home_player_5",
    "away_player_1",
    "away_player_2",
    "away_player_3",
    "away_player_4",
    "away_player_5",
]


def _init_lineup() -> List[Optional[int]]:
    return [None] * 5


def _update_with_player(lineup: List[Optional[int]], player_id: Optional[int]) -> None:
    if not player_id:
        return
    if player_id in lineup:
        return
    for idx, value in enumerate(lineup):
        if value is None:
            lineup[idx] = player_id
            return


def _copy_lineup(lineup: List[Optional[int]]) -> List[Optional[int]]:
    return list(lineup)


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _player_display_name(player: Dict[str, Any]) -> str:
    name = player.get("name")
    if name:
        return str(name)
    first = player.get("firstName") or player.get("first_name") or ""
    last = player.get("lastName") or player.get("last_name") or ""
    full = f"{first} {last}".strip()
    return full


def extract_starters_from_box(box_json: Dict[str, Any]) -> Dict[str, List[int]]:
    """Extract starter ids for each team from a CDN box score payload."""

    def side(team: str) -> List[int]:
        players = (
            box_json.get("game", {})
            .get(team, {})
            .get("players", [])
        )
        starters: List[int] = []
        for player in players:
            pid = _safe_int(player.get("personId"))
            if not pid:
                continue
            if player.get("starter") or player.get("starterPosition"):
                starters.append(pid)
        if len(starters) < 5:
            pool: List[int] = []
            for player in players:
                pid = _safe_int(player.get("personId"))
                if not pid:
                    continue
                status = str(player.get("status") or "").upper()
                if status == "ACTIVE":
                    pool.append(pid)
            for pid in pool:
                if pid not in starters:
                    starters.append(pid)
                    if len(starters) == 5:
                        break
        if len(starters) < 5:
            for player in players:
                pid = _safe_int(player.get("personId"))
                if pid and pid not in starters:
                    starters.append(pid)
                    if len(starters) == 5:
                        break
        return starters if len(starters) == 5 else []

    return {"home": side("homeTeam"), "away": side("awayTeam")}


def _seed_lineup(starters: List[int]) -> List[Optional[int]]:
    lineup = _init_lineup()
    for idx, pid in enumerate(starters[:5]):
        pid_int = _safe_int(pid)
        lineup[idx] = pid_int if pid_int else None
    return lineup


def attach_lineups(
    df: pd.DataFrame,
    starters: Optional[Dict[str, List[int]]] = None,
    box_json: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    if df.empty:
        for column in _LINEUP_ID_COLUMNS + _LINEUP_NAME_COLUMNS:
            df[column] = []
        return df

    df = df.copy()
    home_id = _safe_int(df["home_team_id"].iloc[0])
    away_id = _safe_int(df["away_team_id"].iloc[0])

    if starters is None and box_json is not None:
        starters = extract_starters_from_box(box_json)

    home_starters = starters.get("home", []) if starters else []
    away_starters = starters.get("away", []) if starters else []

    home_lineup = _seed_lineup(home_starters) if len(home_starters) == 5 else _init_lineup()
    away_lineup = _seed_lineup(away_starters) if len(away_starters) == 5 else _init_lineup()

    home_candidates: List[int] = [pid for pid in home_lineup if pid]
    away_candidates: List[int] = [pid for pid in away_lineup if pid]

    home_history: List[List[Optional[int]]] = []
    away_history: List[List[Optional[int]]] = []

    name_map: Dict[int, str] = {}

    for id_col, name_col in [
        ("player1_id", "player1_name"),
        ("player2_id", "player2_name"),
        ("player3_id", "player3_name"),
    ]:
        if id_col not in df.columns or name_col not in df.columns:
            continue
        for pid, name in zip(df[id_col], df[name_col]):
            try:
                pid_int = int(pid)
            except (TypeError, ValueError):
                continue
            if not name:
                continue
            name_map.setdefault(pid_int, str(name))

    if box_json:
        for team in ("homeTeam", "awayTeam"):
            players = (
                box_json.get("game", {})
                .get(team, {})
                .get("players", [])
            )
            for player in players:
                pid = _safe_int(player.get("personId"))
                if not pid:
                    continue
                display_name = _player_display_name(player)
                if display_name:
                    name_map.setdefault(pid, display_name)

    for _, row in df.iterrows():
        team_id = _safe_int(row.get("team_id"))
        player_id = _safe_int(row.get("player1_id"))
        if team_id and home_id and team_id == home_id:
            if player_id and player_id not in home_candidates:
                home_candidates.append(player_id)
                _update_with_player(home_lineup, player_id)
        elif team_id and away_id and team_id == away_id:
            if player_id and player_id not in away_candidates:
                away_candidates.append(player_id)
                _update_with_player(away_lineup, player_id)

        if row.get("family") == "substitution":
            sub_out = _safe_int(row.get("player1_id"))
            sub_in = _safe_int(row.get("player2_id"))
            target = home_lineup if team_id and home_id and team_id == home_id else away_lineup
            if sub_out in target:
                idx = target.index(sub_out)
                target[idx] = sub_in or target[idx]
            elif sub_in and sub_in not in target:
                _update_with_player(target, sub_in)

        home_history.append(_copy_lineup(home_lineup))
        away_history.append(_copy_lineup(away_lineup))

        for id_col, name_col in [
            ("player1_id", "player1_name"),
            ("player2_id", "player2_name"),
            ("player3_id", "player3_name"),
        ]:
            pid = row.get(id_col)
            name = row.get(name_col)
            try:
                pid_int = int(pid)
            except (TypeError, ValueError):
                continue
            if not name:
                continue
            name_map.setdefault(pid_int, str(name))

    for idx in range(5):
        df[f"home_player_{idx+1}_id"] = [line[idx] for line in home_history]
        df[f"away_player_{idx+1}_id"] = [line[idx] for line in away_history]

    def _name_for(pid: Optional[int]) -> str:
        try:
            pid_int = int(pid)
        except (TypeError, ValueError):
            return ""
        return name_map.get(pid_int, "")

    for idx in range(5):
        df[f"home_player_{idx+1}"] = [
            _name_for(line[idx]) for line in home_history
        ]
        df[f"away_player_{idx+1}"] = [
            _name_for(line[idx]) for line in away_history
        ]

    fill_cols = _LINEUP_ID_COLUMNS + _LINEUP_NAME_COLUMNS
    df[fill_cols] = (
        df.groupby("game_id", group_keys=False)[fill_cols]
        .ffill()
        .bfill()
    )

    return df
