[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Maintenance](https://img.shields.io/maintenance/no/2021)](https://github.com/mcbarlowe/nba_scraper/commits/master)
[![PyPI version](https://badge.fury.io/py/nba-scraper.svg)](https://badge.fury.io/py/nba-scraper)
[![Downloads](https://pepy.tech/badge/nba-scraper)](https://pepy.tech/project/nba-scraper)
[![Build Status](https://travis-ci.org/mcbarlowe/nba_scraper.svg?branch=master)](https://travis-ci.org/mcbarlowe/nba_scraper)
[![codecov](https://codecov.io/gh/mcbarlowe/nba_scraper/branch/master/graph/badge.svg)](https://codecov.io/gh/mcbarlowe/nba_scraper)

# This package is no longer maintained as of 2021/01/30. Any outstanding issues or new ones will not be fixed
# `nba_scraper`

This is a package written in Python to scrape the NBA's api and produce the
play by play of games either in a `csv` file or a `pandas` dataframe. This package
has two main functions `scrape_game` which scrapes an individual game or a list
of specific games, and `scrape_season` which scrapes an entire season of regular
season games.

The scraper now supports both the modern NBA CDN live data feeds (2019 and later)
and legacy v2 JSON archives. No matter the source, results are normalised to a
canonical schema that includes structured shot metadata, team context and on-court
lineups computed from the play-by-play itself.

## Canonical dataframe columns

Each parsed play-by-play row exposes a common set of columns that are designed to
interoperate with downstream tools such as `nba_parser`. In addition to the core
fields (`eventmsgtype`, `pctimestring`, `player1_id`, etc.) the dataframe now
includes:

* `event_team` and `event_type_de` – a consistent team abbreviation and event
  label for every action.
* `player1_team_id`, `player2_team_id`, `player3_team_id` – acting and secondary
  team identifiers populated for assists, steals and blocks.
* `is_turnover`, `is_steal`, `is_block` – boolean flags used by possession
  aggregators.
* `ft_n` and `ft_m` – structured free-throw trip counters extracted from the
  feed and normalised for technical/single attempts.
* `season` – derived from the UTC tip-off date.
* `home_player_1`…`home_player_5` / `away_player_1`…`away_player_5` – lineup
  names alongside the existing ID columns.

These compatibility columns are present for both CDN and legacy v2 sources so
that the resulting dataframe can be dropped directly into analytics pipelines.

## Using with `nba_parser`

The canonical dataframe can be passed straight into [`nba_parser`](https://pypi.org/project/nba-parser/)
for advanced possession and box score calculations:

```python
from pathlib import Path

from nba_scraper import io_sources, lineup_builder
from nba_parser import PbP

pbp_path = Path("cdn_playbyplay_0022400001.json")
box_path = Path("cdn_boxscore_0022400001.json")

df = io_sources.parse_any((pbp_path, box_path), io_sources.SourceKind.CDN_LOCAL)
df = lineup_builder.attach_lineups(df)

pbp = PbP(df)
player_totals = pbp.playerbygamestats()
team_totals = pbp.teambygamestats()
```

The example above uses local CDN fixtures, but any canonical dataframe returned
by `nba_scraper` (including legacy v2 games) will work with `nba_parser.PbP`.

# Installation

To install this package just type this at the command line:

    pip install nba_scraper

# Usage

## `scrape_game`

The default data format is a pandas dataframe you can change this to csv
with the `data_format` parameter. The default file path is the
users home directory you can change this with the `data_dir` parameter

    import nba_scraper.nba_scraper as ns

    # if you want to return a dataframe
    # you can pass the function a list of strings or integers
    # all nba game ids have two leading zeros but you can omit these
    # to make it easier to create lists of game ids as I add them on
    nba_df = ns.scrape_game([21800001, 21800002])

    # if you want a csv if you don't pass a file path the default is home
    # directory
    ns.scrape_game([21800001, 21800002], data_format='csv', data_dir='file/path')

## `scrape_from_files`

Local CDN JSON pairs or legacy v2 JSON dumps can be parsed directly:

    ns.scrape_from_files('playbyplay.json', 'boxscore.json', kind='cdn_local')
    ns.scrape_from_files('0021700001.json', kind='v2_local')

## `scrape_season`

The `data_format` and `data_dir` key words are used the excat same way as
`scrape_game`. Instead of game ids though, you would pass the season you want
scraped to the function. This season is a four digit year that must be an
integer.

    import nba_scraper.nba_scraper as ns

    #scrape a season
    nba_df = ns.scrape_season(2019)

    # if you want a csv if you don't pass a file path the default is home
    # directory
    ns.scrape_season(2019, data_format='csv', data_dir='file/path')

## `scrape_date_range`

This allows you to scrape all **regular season** games in the date range passed to
the function. As of right now it will not scrape playoff games. Date format must
be passed in the format `YYYY-MM-DD`.

    import nba_scraper.nba_scraper as ns

    #scrape a season
    nba_df = ns.scrape_date_range('2019-01-01', 2019-01-03')

    # if you want a csv if you don't pass a file path the default is home
    # directory
    ns.scrape_date_range('2019-01-01', 2019-01-03', data_format='csv', data_dir='file/path')

# Contact

If you have any troubles or bugs please **open an issue/bug report**. If you have
any improvements/suggestions please **submit a pull request**. If it falls outside
those two areas please feel free to email me at
[matt@barloweanalytics.com](mailto:matt@barloweanalytics.com).




