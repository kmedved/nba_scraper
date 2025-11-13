import importlib
import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("pandas")

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from nba_scraper import cdn_parser, io_sources, lineup_builder, v2_parser
from nba_scraper.parser_utils import infer_possession_after

FIXTURES = Path(__file__).parent / "test_files"


def _load_json(name: str):
    with (FIXTURES / name).open() as fh:
        return json.load(fh)


def test_cdn_parse_basic():
    pbp = _load_json("cdn_playbyplay_0022400001.json")
    box = _load_json("cdn_boxscore_0022400001.json")
    df = cdn_parser.parse_actions_to_rows(pbp, box)
    df = lineup_builder.attach_lineups(df, box_json=box, pbp_json=pbp)
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


def test_cdn_parser_populates_secondary_names_from_json():
    pbp = _load_json("cdn_playbyplay_0022400001.json")
    box = _load_json("cdn_boxscore_0022400001.json")
    df = cdn_parser.parse_actions_to_rows(pbp, box)

    assist_row = df[(df["assist_id"] != 0) & (df["shot_made"] == 1)].iloc[0]
    assert assist_row["player2_name"] == "Stephen Curry"

    steal_row = df[df["steal_id"] != 0].iloc[0]
    assert steal_row["player2_name"] == "Stephen Curry"

    block_row = df[df["block_id"] != 0].iloc[0]
    assert block_row["player3_name"] == "Paul Millsap"


def test_cdn_scores_populated():
    pbp = _load_json("cdn_playbyplay_0022400001.json")
    box = _load_json("cdn_boxscore_0022400001.json")
    df = cdn_parser.parse_actions_to_rows(pbp, box)
    assert df["score_home"].notna().any()
    assert df["score_away"].notna().any()
    assert df["scoremargin"].str.len().max() > 0


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


def test_lineups_have_five_players_every_live_row():
    pbp = _load_json("cdn_playbyplay_0022400001.json")
    box = _load_json("cdn_boxscore_0022400001.json")
    df = io_sources.parse_any((pbp, box), io_sources.SourceKind.CDN_LOCAL)
    df = lineup_builder.attach_lineups(df, box_json=box, pbp_json=pbp)

    live = ~df["event_type_de"].isin(["period", "timeout"])
    for side in ("home", "away"):
        cols = [f"{side}_player_{i}_id" for i in range(1, 6)]
        numeric = (
            df.loc[live, cols]
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0)
            .astype(int)
        )
        assert ((numeric > 0).sum(axis=1) == 5).all()


def test_cdn_split_substitution_updates_lineups():
    pbp = _load_json("cdn_playbyplay_0022400001.json")
    box = _load_json("cdn_boxscore_0022400001.json")
    df = io_sources.parse_any((pbp, box), io_sources.SourceKind.CDN_LOCAL)
    df = lineup_builder.attach_lineups(df, box_json=box, pbp_json=pbp)

    sub_in = df[(df["family"] == "substitution") & (df["player1_id"] == 1626204)].iloc[0]
    idx = sub_in.name

    home_ids = [df.loc[idx, f"home_player_{i}_id"] for i in range(1, 6)]
    assert 1626204 in home_ids


def test_team_fields_filled_on_team_events():
    pbp = _load_json("cdn_playbyplay_0022400001.json")
    box = _load_json("cdn_boxscore_0022400001.json")
    df = io_sources.parse_any((pbp, box), io_sources.SourceKind.CDN_LOCAL)

    team_events = ~df["event_type_de"].isin(["period", "timeout", "jump-ball"])
    team_ids = (
        pd.to_numeric(df.loc[team_events, "team_id"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    assert (team_ids != 0).all()
    tricodes = df.loc[team_events, "team_tricode"].fillna("")
    assert (tricodes != "").all()


def test_shots_have_xy_or_are_synthesized():
    pbp = _load_json("cdn_playbyplay_0022400001.json")
    box = _load_json("cdn_boxscore_0022400001.json")
    df = io_sources.parse_any((pbp, box), io_sources.SourceKind.CDN_LOCAL)

    shots = df["family"].isin(["2pt", "3pt"])
    xy_present = shots & df["x"].notna() & df["y"].notna()

    def _has_flag(flags):
        if isinstance(flags, (list, tuple, set)):
            return "xy_synth" in flags
        return False

    synth = shots & df["style_flags"].apply(_has_flag)
    assert (xy_present | synth)[shots].all()


def test_lineup_builder_substitution_uses_player1_team_id_when_team_missing():
    home_id = 1610612737
    away_id = 1610612744
    df = pd.DataFrame(
        [
            {
                "game_id": "test-game",
                "period": 1,
                "family": "rebound",
                "team_id": home_id,
                "player1_id": 2001,
                "player1_team_id": home_id,
                "player2_id": 0,
                "player3_id": 0,
                "player2_team_id": 0,
                "player3_team_id": 0,
                "player1_name": "",
                "player2_name": "",
                "player3_name": "",
                "home_team_id": home_id,
                "away_team_id": away_id,
            },
            {
                "game_id": "test-game",
                "period": 1,
                "family": "substitution",
                "team_id": 0,
                "player1_id": 2001,
                "player2_id": 2002,
                "player1_team_id": home_id,
                "player2_team_id": 0,
                "player3_id": 0,
                "player3_team_id": 0,
                "player1_name": "",
                "player2_name": "",
                "player3_name": "",
                "home_team_id": home_id,
                "away_team_id": away_id,
            },
        ]
    )

    result = lineup_builder.attach_lineups(df)

    assert result.loc[0, "home_player_1_id"] == 2001
    assert result.loc[1, "home_player_1_id"] == 2002


def test_possession_after_filled_between_anchors():
    pbp = _load_json("cdn_playbyplay_0022400001.json")
    box = _load_json("cdn_boxscore_0022400001.json")
    df = io_sources.parse_any((pbp, box), io_sources.SourceKind.CDN_LOCAL)
    df = infer_possession_after(df)

    by_period = df.groupby(["game_id", "period"])
    live = ~df["event_type_de"].isin(["period", "timeout"])
    mask = live & df["possession_after"].notna()
    checks = []
    for _, group in by_period:
        live_mask = live.loc[group.index]
        live_vals = mask.loc[group.index][live_mask]
        if live_vals.empty:
            checks.append(True)
            continue
        checks.append(live_vals.iloc[1:].all())
    assert all(checks)
