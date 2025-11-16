from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("pandas")

from nba_scraper import boxscore_validation, cdn_parser  # noqa: E402


FIXTURES = Path(__file__).parent / "test_files"


def _load_json(name: str):
    with (FIXTURES / name).open(encoding="utf-8") as fh:
        return json.load(fh)


def test_boxscore_validation_matches_fixture():
    """
    Sanity check: for the fixture game, PbP-derived team totals should match
    the official boxscore for core shooting stats.

    This acts as a regression harness for future parser changes that might
    accidentally drop or misclassify events.
    """
    pbp = _load_json("cdn_playbyplay_0022400001.json")
    box = _load_json("cdn_boxscore_0022400001.json")

    df = cdn_parser.parse_actions_to_rows(pbp, box)
    mismatches = boxscore_validation.compare_pbp_to_box(df, box, atol=0)

    assert mismatches == []


def test_boxscore_validation_handles_empty_inputs():
    """
    Defensive behavior on empty dataframes / malformed boxscore payloads.
    """
    import pandas as pd

    empty_df = pd.DataFrame(
        columns=["game_id", "team_id", "family", "shot_made", "points_made"]
    )
    mismatches = boxscore_validation.compare_pbp_to_box(empty_df, {}, atol=0)
    assert mismatches == []
