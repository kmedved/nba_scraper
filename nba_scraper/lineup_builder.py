"""Utilities for attaching on-court lineups to the canonical dataframe."""
from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd

_LINEUP_COLUMNS = [
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


def attach_lineups(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        for column in _LINEUP_COLUMNS:
            df[column] = []
        return df

    df = df.copy()
    home_id = df["home_team_id"].iloc[0]
    away_id = df["away_team_id"].iloc[0]

    home_lineup = _init_lineup()
    away_lineup = _init_lineup()

    home_candidates: List[int] = []
    away_candidates: List[int] = []

    home_history: List[List[Optional[int]]] = []
    away_history: List[List[Optional[int]]] = []

    for _, row in df.iterrows():
        team_id = row.get("team_id")
        player_id = row.get("player1_id")
        if team_id == home_id:
            if player_id and player_id not in home_candidates:
                home_candidates.append(player_id)
                _update_with_player(home_lineup, player_id)
        elif team_id == away_id:
            if player_id and player_id not in away_candidates:
                away_candidates.append(player_id)
                _update_with_player(away_lineup, player_id)

        if row.get("family") == "substitution":
            sub_out = row.get("player1_id")
            sub_in = row.get("player2_id")
            target = home_lineup if row.get("team_id") == home_id else away_lineup
            if sub_out in target:
                idx = target.index(sub_out)
                target[idx] = sub_in or target[idx]
            elif sub_in and sub_in not in target:
                _update_with_player(target, sub_in)

        home_history.append(_copy_lineup(home_lineup))
        away_history.append(_copy_lineup(away_lineup))

    for idx in range(5):
        df[f"home_player_{idx+1}_id"] = [line[idx] for line in home_history]
        df[f"away_player_{idx+1}_id"] = [line[idx] for line in away_history]

    return df
