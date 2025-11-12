import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

pytest.importorskip("nba_parser")
pytest.importorskip("pandas")

import pandas as pd  # noqa: E402

from nba_scraper import io_sources, lineup_builder  # noqa: E402

FIXTURES = Path(__file__).parent / "test_files"


def test_cdn_frame_works_with_nba_parser():
    from nba_parser import PbP

    pbp = FIXTURES / "cdn_playbyplay_0022400001.json"
    box = FIXTURES / "cdn_boxscore_0022400001.json"

    df = io_sources.parse_any((pbp, box), io_sources.SourceKind.CDN_LOCAL)
    df = lineup_builder.attach_lineups(df)

    required = {
        "event_type_de",
        "event_team",
        "player1_team_id",
        "player2_team_id",
        "player3_team_id",
        "is_turnover",
        "is_steal",
        "is_block",
        "season",
        "home_player_1_id",
        "away_player_5_id",
    }
    assert required.issubset(df.columns)

    pbp = PbP(df)
    pbg = pbp.playerbygamestats()
    tbg = pbp.teambygamestats()

    assert not pbg.empty
    assert not tbg.empty
