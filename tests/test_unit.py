import importlib
import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("pandas")

sys.path.append(str(Path(__file__).resolve().parents[1]))

from nba_scraper import cdn_parser, lineup_builder, v2_parser

FIXTURES = Path(__file__).parent / "test_files"


def _load_json(name: str):
    with (FIXTURES / name).open() as fh:
        return json.load(fh)


def test_cdn_parse_basic():
    pbp = _load_json("cdn_playbyplay_0022400001.json")
    box = _load_json("cdn_boxscore_0022400001.json")
    df = cdn_parser.parse_actions_to_rows(pbp, box)
    df = lineup_builder.attach_lineups(df)
    assert not df.empty
    assert "home_player_1_id" in df.columns
    assert {"eventnum", "season"}.issubset(df.columns)
    expected_columns = {
        "game_id",
        "period",
        "pctimestring",
        "seconds_elapsed",
        "eventmsgtype",
        "eventmsgactiontype",
        "event_type_de",
        "event_team",
        "player1_team_id",
        "player2_team_id",
        "player3_team_id",
        "points_made",
        "steal_id",
        "block_id",
        "is_turnover",
        "is_steal",
        "is_block",
        "ft_n",
        "ft_m",
        "season",
        "home_player_1",
        "away_player_1",
    }
    assert expected_columns.issubset(df.columns)
    turnover_row = df[df["family"] == "turnover"].iloc[0]
    assert turnover_row["steal_id"] == 201939
    assert turnover_row["is_turnover"] == 1
    assert turnover_row["is_steal"] == 1
    shot_row = df[df["family"] == "2pt"].iloc[0]
    assert shot_row["block_id"] == 201143
    assert shot_row["is_block"] == 1


def test_cdn_game_date_normalized():
    pbp = _load_json("cdn_playbyplay_0022400001.json")
    box = _load_json("cdn_boxscore_0022400001.json")
    df = cdn_parser.parse_actions_to_rows(pbp, box)
    val = str(df["game_date"].iloc[0])
    assert len(val) == 10


def test_eventnum_is_numeric():
    v2 = _load_json("v2_pbp_0021700001.json")
    df2 = v2_parser.parse_v2_to_rows(v2)
    assert "eventnum" in df2.columns
    assert df2["eventnum"].dtype.kind in ("i", "u")

    pbp = _load_json("cdn_playbyplay_0022400001.json")
    box = _load_json("cdn_boxscore_0022400001.json")
    dfc = cdn_parser.parse_actions_to_rows(pbp, box)
    assert "eventnum" in dfc.columns
    assert dfc["eventnum"].dtype.kind in ("i", "u")


def test_v2_parse_basic():
    v2 = _load_json("v2_pbp_0021700001.json")
    df = v2_parser.parse_v2_to_rows(v2)
    assert not df.empty
    assert {"eventnum", "season"}.issubset(df.columns)
    assert set(
        [
            "family",
            "eventmsgtype",
            "points_made",
            "event_type_de",
            "event_team",
            "player1_team_id",
            "season",
        ]
    ).issubset(df.columns)
    made = df[df["eventmsgtype"] == 1].iloc[0]
    assert made["points_made"] == 2
    assert made["event_type_de"] == "shot"


def test_v2_game_date_normalized():
    v2 = _load_json("v2_pbp_0021700001.json")
    df = v2_parser.parse_v2_to_rows(v2)
    val = str(df["game_date"].iloc[0])
    assert len(val) == 10


def test_seconds_elapsed():
    pbp = _load_json("cdn_playbyplay_0022400001.json")
    box = _load_json("cdn_boxscore_0022400001.json")
    df = cdn_parser.parse_actions_to_rows(pbp, box)
    first = df.iloc[0]
    assert first["seconds_elapsed"] == 0
    shot = df[df["action_number"] == 3].iloc[0]
    assert shot["seconds_elapsed"] > first["seconds_elapsed"]


def test_points_made():
    pbp = _load_json("cdn_playbyplay_0022400001.json")
    box = _load_json("cdn_boxscore_0022400001.json")
    df = cdn_parser.parse_actions_to_rows(pbp, box)
    three = df[df["action_number"] == 3].iloc[0]
    assert three["points_made"] == 3
    layup = df[df["action_number"] == 7].iloc[0]
    assert layup["points_made"] == 0


def test_sidecar_merge():
    pbp = _load_json("cdn_playbyplay_0022400001.json")
    box = _load_json("cdn_boxscore_0022400001.json")
    df = cdn_parser.parse_actions_to_rows(pbp, box)
    action_numbers = set(df["action_number"].tolist())
    assert 5 not in action_numbers  # steal sidecar merged
    assert 8 not in action_numbers  # block sidecar merged


def test_cdn_synth_ft_description(monkeypatch):
    monkeypatch.setenv("NBA_SCRAPER_SYNTH_FT_DESC", "1")
    # Reload module to pick up environment variable change.
    module = importlib.reload(cdn_parser)
    globals()["cdn_parser"] = module
    pbp = _load_json("cdn_playbyplay_0022400001.json")
    box = _load_json("cdn_boxscore_0022400001.json")
    df = module.parse_actions_to_rows(pbp, box)
    ft_rows = df[df["family"] == "freethrow"]
    assert not ft_rows.empty
    has_desc = ft_rows[
        (ft_rows["homedescription"] != "") | (ft_rows["visitordescription"] != "")
    ]
    assert not has_desc.empty
    # Reset module state for other tests
    monkeypatch.setenv("NBA_SCRAPER_SYNTH_FT_DESC", "0")
    module = importlib.reload(module)
    globals()["cdn_parser"] = module
