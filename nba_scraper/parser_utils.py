"""Shared helpers for parser post-processing tasks."""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import pandas as pd

_ABOVE: Dict[str, Tuple[float, float]] = {
    "Left": (25.0, 82.0),
    "Left Center": (35.0, 82.0),
    "Center": (50.0, 82.0),
    "Right Center": (65.0, 82.0),
    "Right": (75.0, 82.0),
}

_MID: Dict[str, Tuple[float, float]] = {
    "8-16 Left": (30.0, 55.0),
    "8-16 Center": (50.0, 55.0),
    "8-16 Right": (70.0, 55.0),
    "16-24 Left": (30.0, 68.0),
    "16-24 Center": (50.0, 68.0),
    "16-24 Right": (70.0, 68.0),
}

_PAINT: Dict[str, Tuple[float, float]] = {
    "0-8 Left": (45.0, 30.0),
    "0-8 Center": (50.0, 30.0),
    "0-8 Right": (55.0, 30.0),
}

_CORN: Dict[str, Tuple[float, float]] = {
    "Left": (8.0, 10.0),
    "Right": (92.0, 10.0),
}

_RA: Dict[str, Tuple[float, float]] = {"0-8 Center": (50.0, 22.0)}


def _fill_team_fields(row: Dict[str, Any]) -> None:
    """Ensure team identifiers are populated for team-anchored events."""

    if not row.get("team_id"):
        if row.get("player1_team_id"):
            row["team_id"] = row["player1_team_id"]
        elif row.get("event_team"):
            if row["event_team"] == row.get("home_team_abbrev"):
                row["team_id"] = row.get("home_team_id")
            elif row["event_team"] == row.get("away_team_abbrev"):
                row["team_id"] = row.get("away_team_id")

    if not row.get("team_tricode"):
        if row.get("team_id") == row.get("home_team_id"):
            row["team_tricode"] = row.get("home_team_abbrev")
        elif row.get("team_id") == row.get("away_team_id"):
            row["team_tricode"] = row.get("away_team_abbrev")

    if not row.get("event_team"):
        row["event_team"] = row.get("team_tricode") or ""


def _synth_xy(area: str, area_detail: str, side: str) -> tuple[Optional[float], Optional[float]]:
    area_lower = (area or "").lower()
    detail = area_detail or ""
    side_norm = (side or "").capitalize()
    if "restricted" in area_lower:
        return _RA.get("0-8 Center", (None, None))
    if "non-ra" in area_lower or "paint" in area_lower:
        return _PAINT.get(detail, (None, None))
    if "mid-range" in area_lower:
        return _MID.get(detail, (None, None))
    if "corner 3" in area_lower:
        return _CORN.get(side_norm, (None, None))
    if "above the break 3" in area_lower or "above the break" in area_lower:
        key = detail if detail else side_norm
        return _ABOVE.get(key, (None, None))
    return (None, None)


def infer_possession_after(df: pd.DataFrame) -> pd.DataFrame:
    """Fill in missing possession_after values between explicit anchors."""

    df = df.copy()
    poss = df["possession_after"].copy()

    def _next_possession(row: pd.Series) -> Optional[int]:
        def _safe_int(value: Any) -> int:
            if pd.isna(value):
                return 0
            try:
                return int(value)
            except (TypeError, ValueError):
                return 0

        team_id = _safe_int(row.get("team_id"))
        event_type = _safe_int(row.get("eventmsgtype"))
        family = row.get("family")

        home_team = _safe_int(row.get("home_team_id"))
        away_team = _safe_int(row.get("away_team_id"))

        def _opponent(tid: int) -> Optional[int]:
            if tid == 0 or home_team == 0 or away_team == 0:
                return None
            return away_team if tid == home_team else home_team

        shot_made = _safe_int(row.get("shot_made"))
        ft_n_val = _safe_int(row.get("ft_n"))
        ft_m_val = _safe_int(row.get("ft_m"))

        # 1) First, try to infer from the event itself.
        hint: Optional[int] = None

        # Turnover: ball goes to the opponent.
        if event_type == 5:  # turnover
            opp = _opponent(team_id)
            if opp:
                hint = opp

        # Made field goal: ball goes to the opponent.
        elif family in {"2pt", "3pt"} and shot_made == 1:
            opp = _opponent(team_id)
            if opp:
                hint = opp

        # Defensive rebound: rebounder takes possession.
        elif family == "rebound" and _safe_int(row.get("is_d_rebound")) == 1:
            hint = team_id or None

        # Last made FT of a trip: ball goes to the opponent.
        elif (
            family == "freethrow"
            and ft_n_val
            and ft_m_val
            and ft_n_val == ft_m_val
            and shot_made == 1
        ):
            opp = _opponent(team_id)
            if opp:
                hint = opp

        if hint:
            return int(hint)

        # 2) If we couldn't infer, fall back to the raw possession field.
        poss_val = row.get("possession_after")
        if pd.notna(poss_val) and poss_val not in ("", 0):
            try:
                return int(poss_val)
            except (TypeError, ValueError):
                return None

        return None

    hints = df.apply(_next_possession, axis=1)
    poss = poss.where(poss.notna() & (poss != 0), hints)

    # Do not propagate possession onto period/timeout rows
    live_mask = ~df["event_type_de"].isin(["period", "timeout"])

    # Only fill within live events, grouped by game+period
    poss_live = poss.where(live_mask)
    poss_live = poss_live.groupby([df["game_id"], df["period"]]).ffill().bfill()

    # Put live values back; keep period/timeout at whatever they had (usually NaN/0)
    df["possession_after"] = poss.where(~live_mask, poss_live)
    df["possession_after"] = df["possession_after"].infer_objects(copy=False)
    return df


__all__ = [
    "_fill_team_fields",
    "_synth_xy",
    "infer_possession_after",
]
