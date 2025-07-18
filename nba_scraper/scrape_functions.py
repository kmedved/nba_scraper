"""
Date: 2019-01-02
Contributor: Matthew Barlowe
Twitter: @barloweanalytic
Email: matt@barloweanalytics.com

This file contains the main functions to scrape and compile the NBA api and
return a CSV file of the pbp for the provided game
"""
import sys
import json
import datetime
import requests
import pandas as pd
import numpy as np

# TODO probably need to fix these to import modularly correctly
from nba_scraper.helper_functions import EVENT_TYPE_DICT, get_season
from nba_scraper.stat_calc_functions import (
    made_shot,
    parse_foul,
    parse_shot_types,
    create_seconds_elapsed,
    calc_points_made,
)

# TODO look at replacing this with the fake-useragent package Matt Barlowe 2019-12-04
# have to pass this to the requests function or the api will return a 403 code
SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": requests.utils.default_user_agent(),
        "Accept": "application/json, text/plain, */*",
        "x-nba-stats-origin": "stats",
        "x-nba-stats-token": "true",
        "Referer": "https://www.nba.com",
    }
)




def scrape_pbp(v2_dict):
    """
    This function scrapes both of the pbp urls and returns a joined/cleaned
    pbp dataframe

    Inputs:
    v2_dict     - stats.nba.com api response

    Outputs:
    clean_df - final cleaned dataframe
    """

    # converting stats.nba.com json into pandas dataframe
    pbp_v2_df = pd.DataFrame(
        v2_dict["resultSets"][0]["rowSet"], columns=v2_dict["resultSets"][0]["headers"]
    )
    pbp_v2_df.columns = list(map(str.lower, pbp_v2_df.columns))

    # pulling the home and away team abbreviations and the game date

    jump_ball = pbp_v2_df[pbp_v2_df["eventmsgtype"] == 10].iloc[0]
    if pd.isnull(jump_ball["homedescription"]):
        home_team_abbrev = jump_ball["player2_team_abbreviation"]
        away_team_abbrev = jump_ball["player1_team_abbreviation"]
    else:
        home_team_abbrev = jump_ball["player1_team_abbreviation"]
        away_team_abbrev = jump_ball["player2_team_abbreviation"]

    pbp_v2_df["home_team_abbrev"] = home_team_abbrev
    pbp_v2_df["away_team_abbrev"] = away_team_abbrev

    clean_df = pbp_v2_df

    print(
        clean_df[clean_df["player1_team_abbreviation"] == home_team_abbrev][
            "player1_team_id"
        ]
        .astype(int)
        .unique()
    )
    # code to properly get the team ids as the scientific notation cuts off some digits
    clean_df.loc[:, "home_team_id"] = (
        clean_df[clean_df["player1_team_abbreviation"] == home_team_abbrev][
            "player1_team_id"
        ]
        .astype(int)
        .unique()[0]
    )

    clean_df.loc[:, "away_team_id"] = (
        clean_df[clean_df["player1_team_abbreviation"] == away_team_abbrev][
            "player1_team_id"
        ]
        .astype(int)
        .unique()[0]
    )

    clean_df["game_date"] = ""

    if clean_df["game_id"].unique()[0][3:5] == "99":
        clean_df["season"] = 2000
    elif clean_df["game_id"].unique()[0][3:5] == "00":
        clean_df["season"] = 2001
    else:
        clean_df.loc[
            :, ("season")
        ] = f"20{int(clean_df['game_id'].unique()[0][3:5])+1:02}"
    # TODO columns to pull out [['evt', 'locX', 'locY', 'hs', 'vs', 'de']]
    # create an event team colum
    clean_df["event_team"] = np.where(
        clean_df["homedescription"].isnull(),
        clean_df["away_team_abbrev"],
        np.where(
            clean_df["visitordescription"].isnull(),
            clean_df["home_team_abbrev"],
            np.where(
                (clean_df["homedescription"].str.contains("Turnover"))
                | (clean_df["homedescription"].str.contains("MISS")),
                clean_df["home_team_abbrev"],
                clean_df["away_team_abbrev"],
            ),
        ),
    )

    # create and event type description column
    clean_df["event_type_de"] = clean_df[["eventmsgtype"]].replace(
        {"eventmsgtype": EVENT_TYPE_DICT}
    )

    # DON'T DELETE THIS WILL BRAKE WHOLE PROGRAM
    clean_df["shot_type_de"] = ""

    # create column whether shot was succesful or not
    clean_df["shot_made"] = clean_df.apply(made_shot, axis=1)

    # create a column that says whether the shot was blocked or not
    clean_df["is_block"] = np.where(
        clean_df["homedescription"].str.contains("BLOCK")
        | clean_df["visitordescription"].str.contains("BLOCK"),
        1,
        0,
    )
    # parse mtype column to get all the shot types being taken
    clean_df["shot_type"] = clean_df.apply(parse_shot_types, axis=1)

    # Clean time to get a seconds elapsed column
    clean_df["seconds_elapsed"] = clean_df.apply(create_seconds_elapsed, axis=1)

    # calculate event length of each event in seconds
    clean_df["event_length"] = clean_df["seconds_elapsed"] - clean_df[
        "seconds_elapsed"
    ].shift(1)

    # determine whether shot was a three pointer
    clean_df["is_three"] = np.where(
        (clean_df["homedescription"].str.contains("3PT")).fillna(False)
        | (clean_df["visitordescription"].str.contains("3PT")).fillna(False),
        1,
        0,
    )

    # determine points earned
    clean_df["points_made"] = clean_df.apply(calc_points_made, axis=1)

    # create columns that determine if rebound is offenseive or deffensive
    clean_df["is_o_rebound"] = np.where(
        (clean_df["event_type_de"] == "rebound")
        & (clean_df["event_team"] == clean_df["event_team"].shift(1))
        & (
            ~clean_df["player1_id"].isin(
                [clean_df.home_team_id.unique()[0], clean_df.away_team_id.unique()[0]]
            )
        ),
        1,
        0,
    )
    clean_df["is_d_rebound"] = np.where(
        (clean_df["event_type_de"] == "rebound")
        & (clean_df["event_team"] != clean_df["event_team"].shift(1))
        & (
            ~clean_df["player1_id"].isin(
                [clean_df.home_team_id.unique()[0], clean_df.away_team_id.unique()[0]]
            )
        ),
        1,
        0,
    )

    # create columns to determine turnovers and steals
    clean_df["is_turnover"] = np.where(
        (clean_df["homedescription"].str.contains("Turnover")).fillna(False)
        | (clean_df["visitordescription"].str.contains("Turnover")).fillna(False),
        1,
        0,
    )
    clean_df["is_steal"] = np.where(
        (clean_df["homedescription"].str.contains("STEAL")).fillna(False)
        | (clean_df["visitordescription"].str.contains("STEAL")).fillna(False),
        1,
        0,
    )

    # determine what type of fouls are being commited
    clean_df["foul_type"] = clean_df.apply(parse_foul, axis=1)

    # determine if a shot is a putback off an offensive reboundk
    clean_df["is_putback"] = np.where(
        (clean_df["is_o_rebound"].shift(1) == 1) & (clean_df["event_length"] <= 3), 1, 0
    )

    return clean_df


def get_pbp_api(game_id):
    """
    function gets both JSON requests from the two different APIs if both
    are available and only the stats.nba.com api if not.

    Inputs:
    game_id          - String representing game id

    Outputs:
    v2_dict          - Dictionary of the JSON response from the stats.nba.com api
    """
    v2_api_url = (
        "https://cdn.nba.com/static/json/liveData/playbyplay/"
        f"playbyplay_{game_id}.json"
    )

    try:
        v2_rep = SESSION.get(v2_api_url)
    except json.decoder.JSONDecodeError as ex:
        print(ex)
        print(f"This is the stats.nba.com API's output: {v2_rep.text}")
        sys.exit()

    v2_dict = v2_rep.json()

    return v2_dict


def get_lineup_api(game_id, period):
    """
    function pulls the possible lineups for the given period and game id for
    both the away and home teams

    Inputs:
    game_id            - id of game
    period             - period of game

    Outputs:
    lineup_req_dict    - dictionary of lineup api request respons
    """

    if period <= 4:
        start_range = (((period - 1) * 720) * 10) + 5
        if game_id == "0020100810" and period == 4:
            end_range = start_range + 3000
        else:
            end_range = start_range + 1000
    else:
        start_range = ((((period - 5) * 300) + 2880) * 10) + 5
        end_range = start_range + 1000

    url = (
        f"https://stats.nba.com/stats/boxscoreadvancedv3?gameId={game_id}&"
        f"startPeriod={period}&endPeriod={period}&startRange={start_range}&"
        f"endRange={end_range}&rangeType=2"
    )

    lineups_req = SESSION.get(url)
    lineup_req_dict = json.loads(lineups_req.text)

    return lineup_req_dict


def get_lineup(period_df, lineups, dataframe):
    """
    this function calculates the lineups for each team at each event and then
    appends it to the current dataframe. This only works for one period at a
    time

    Inputs:
    period_df         - the main game pbp dataframe subsetted to only one period
                        in the game
    lineups           - lineup api response dictionary
    dataframe         - full game dataframe. This is passed to get players name from
                        id in case the player didn't have an event in that period.

    Outputs:
    lineup_df     - period_df with each teams lineups calculate and added to the
                    dataframe
    """

    # subsets main dataframe by period and subsets into a home and away subs
    subs_df = period_df[(period_df.event_type_de == "substitution")]
    away_subs = subs_df[pd.isnull(subs_df["visitordescription"]) == 0]
    home_subs = subs_df[pd.isnull(subs_df["homedescription"]) == 0]

    home_team = period_df["home_team_id"].unique()[0]
    away_team = period_df["away_team_id"].unique()[0]
    players = lineups["resultSets"][0]["rowSet"]
    home_ids_names = [(p[4], p[5]) for p in players if p[1] == home_team]
    away_ids_names = [(p[4], p[5]) for p in players if p[1] == away_team]

    # gets the index of the first sub for home and away to get the players who started
    # the period by subsetting the dataframe to all actions before the first sub for
    # each team
    away_indexes = list(away_subs.index)
    home_indexes = list(home_subs.index)
    # pulls the unique values from the whole period dataframe if there are no subs
    # then it just pulls the unique ids from the from the dataframe itself because
    # the away/home indexes will be an empty list

    try:
        home_starting_line = list(
            period_df[
                (period_df.event_team == period_df["home_team_abbrev"].unique()[0])
                & (~pd.isnull(period_df["player1_name"]))
                & (
                    period_df["player1_team_abbreviation"]
                    == period_df["home_team_abbrev"].unique()[0]
                )
                & (period_df.is_block == 0)
                & (period_df.is_steal == 0)
            ]
            .loc[: home_indexes[0], :]["player1_id"]
            .unique()
        )
    except IndexError:
        home_starting_line = list(
            period_df[
                (period_df.event_team == period_df["home_team_abbrev"].unique()[0])
                & (~pd.isnull(period_df["player1_name"]))
                & (
                    period_df["player1_team_abbreviation"]
                    == period_df["home_team_abbrev"].unique()[0]
                )
                & (period_df.is_block == 0)
                & (period_df.is_steal == 0)
            ]["player1_id"].unique()
        )

    if {x for x in home_starting_line} != {x[0] for x in home_ids_names} or len(
        home_ids_names
    ) != 5:
        starting_lineup = set()
        subs = set()
        for i in range(period_df.shape[0]):
            if (
                period_df.iloc[i, :]["event_team"]
                == period_df["home_team_abbrev"].unique()[0]
                and pd.isnull(period_df.iloc[i, :]["player1_name"]) != 1
                and period_df.iloc[i, :]["player1_team_abbreviation"]
                == period_df.iloc[i, :]["home_team_abbrev"]
                and period_df.iloc[i, :]["is_block"] == 0
                and period_df.iloc[i, :]["is_steal"] == 0
            ):
                if period_df.iloc[i, :]["event_type_de"] != "substitution":
                    if (
                        period_df.iloc[i, :]["player1_id"] != 0
                        and period_df.iloc[i, :]["player1_id"] not in subs
                    ):
                        starting_lineup.add(period_df.iloc[i, :]["player1_id"])
                else:
                    if period_df.iloc[i, :]["player2_id"] not in starting_lineup:
                        subs.add(period_df.iloc[i, :]["player2_id"])
                    if period_df.iloc[i, :]["player1_id"] not in subs:
                        starting_lineup.add(period_df.iloc[i, :]["player1_id"])

                if len(starting_lineup) == 5:
                    break
        if len(home_ids_names) < 5:
            home_ids_names = [
                (
                    period_df[period_df.player1_id == x].player1_id.unique()[0],
                    period_df[period_df.player1_id == x].player1_name.unique()[0],
                )
                for x in starting_lineup
            ]
        else:
            home_ids_names = [(p[0], p[1]) for p in home_ids_names if p[0] not in subs]
        if len(home_ids_names) != 5 and len(starting_lineup) == 5:
            home_ids_names = [
                (
                    period_df[period_df.player1_id == x].player1_id.unique()[0],
                    period_df[period_df.player1_id == x].player1_name.unique()[0],
                )
                for x in starting_lineup
            ]

    try:
        away_starting_line = list(
            period_df[
                (period_df.event_team == period_df["away_team_abbrev"].unique()[0])
                & (~pd.isnull(period_df["player1_name"]))
                & (
                    period_df["player1_team_abbreviation"]
                    == period_df["away_team_abbrev"].unique()[0]
                )
                & (period_df.is_block == 0)
                & (period_df.is_steal == 0)
            ]
            .loc[: away_indexes[0], :]["player1_id"]
            .unique()
        )
    except IndexError:
        away_starting_line = list(
            period_df[
                (period_df.event_team == period_df["away_team_abbrev"].unique()[0])
                & (~pd.isnull(period_df["player1_name"]))
                & (
                    period_df["player1_team_abbreviation"]
                    == period_df["away_team_abbrev"].unique()[0]
                )
                & (period_df.is_block == 0)
                & (period_df.is_steal == 0)
            ]["player1_id"].unique()
        )
    if {x for x in away_starting_line} != {x[0] for x in away_ids_names} or len(
        away_ids_names
    ) != 5:
        starting_lineup = set()
        subs = set()
        for i in range(period_df.shape[0]):
            if (
                period_df.iloc[i, :]["event_team"]
                == period_df["away_team_abbrev"].unique()[0]
                and pd.isnull(period_df.iloc[i, :]["player1_name"]) != 1
                and period_df.iloc[i, :]["player1_team_abbreviation"]
                == period_df.iloc[i, :]["away_team_abbrev"]
                and period_df.iloc[i, :]["is_block"] == 0
                and period_df.iloc[i, :]["is_steal"] == 0
            ):
                if period_df.iloc[i, :]["event_type_de"] != "substitution":
                    if (
                        period_df.iloc[i, :]["player1_id"] != 0
                        and period_df.iloc[i, :]["player1_id"] not in subs
                    ):
                        starting_lineup.add(period_df.iloc[i, :]["player1_id"])
                else:
                    if period_df.iloc[i, :]["player2_id"] not in starting_lineup:
                        subs.add(period_df.iloc[i, :]["player2_id"])
                    if period_df.iloc[i, :]["player1_id"] not in subs:
                        starting_lineup.add(period_df.iloc[i, :]["player1_id"])

                if len(starting_lineup) == 5:
                    break
        if len(away_ids_names) < 5:
            away_ids_names = [
                (
                    period_df[period_df.player1_id == x].player1_id.unique()[0],
                    period_df[period_df.player1_id == x].player1_name.unique()[0],
                )
                for x in starting_lineup
            ]
        else:
            away_ids_names = [(p[0], p[1]) for p in away_ids_names if p[0] not in subs]
        if len(away_ids_names) != 5 and len(starting_lineup) == 5:
            away_ids_names = [
                (
                    period_df[period_df.player1_id == x].player1_id.unique()[0],
                    period_df[period_df.player1_id == x].player1_name.unique()[0],
                )
                for x in starting_lineup
            ]

    # creating columns to populate with players on the court
    period_df.loc[:, "home_player_1"] = ""
    period_df.loc[:, "home_player_1_id"] = ""
    period_df.loc[:, "home_player_2"] = ""
    period_df.loc[:, "home_player_2_id"] = ""
    period_df.loc[:, "home_player_3"] = ""
    period_df.loc[:, "home_player_3_id"] = ""
    period_df.loc[:, "home_player_4"] = ""
    period_df.loc[:, "home_player_4_id"] = ""
    period_df.loc[:, "home_player_5"] = ""
    period_df.loc[:, "home_player_5_id"] = ""
    period_df.loc[:, "away_player_1"] = ""
    period_df.loc[:, "away_player_1_id"] = ""
    period_df.loc[:, "away_player_2"] = ""
    period_df.loc[:, "away_player_2_id"] = ""
    period_df.loc[:, "away_player_3"] = ""
    period_df.loc[:, "away_player_3_id"] = ""
    period_df.loc[:, "away_player_4"] = ""
    period_df.loc[:, "away_player_4_id"] = ""
    period_df.loc[:, "away_player_5"] = ""
    period_df.loc[:, "away_player_5_id"] = ""
    # add players to the columns by looping through the dataframe and putting the
    # players in for each row using the starting lineup list. If there is a
    # substitution event then the player coming on replaces the player going off in
    # the list this is done for the whole period
    if (
        period_df.game_id.unique()[0] == "0020200992"
        and period_df.period.unique()[0] == 5
    ):
        away_ids_names.append((922, "Elden Campbell"))

    for i in range(period_df.shape[0]):
        if (
            period_df.iloc[i, :]["event_type_de"] == "substitution"
            and pd.isnull(period_df.iloc[i, :]["visitordescription"]) == 1
        ):
            home_ids_names = [
                ids
                for ids in home_ids_names
                if ids[0] != period_df.iloc[i, :]["player1_id"]
            ]
            home_ids_names.append((period_df.iloc[i, 20], period_df.iloc[i, 21]))
            period_df.loc[period_df.index[i], "home_player_1_id"] = home_ids_names[0][0]
            period_df.loc[period_df.index[i], "home_player_1"] = home_ids_names[0][1]
            period_df.loc[period_df.index[i], "home_player_2_id"] = home_ids_names[1][0]
            period_df.loc[period_df.index[i], "home_player_2"] = home_ids_names[1][1]
            period_df.loc[period_df.index[i], "home_player_3_id"] = home_ids_names[2][0]
            period_df.loc[period_df.index[i], "home_player_3"] = home_ids_names[2][1]
            period_df.loc[period_df.index[i], "home_player_4_id"] = home_ids_names[3][0]
            period_df.loc[period_df.index[i], "home_player_4"] = home_ids_names[3][1]
            period_df.loc[period_df.index[i], "home_player_5_id"] = home_ids_names[4][0]
            period_df.loc[period_df.index[i], "home_player_5"] = home_ids_names[4][1]
            period_df.loc[period_df.index[i], "away_player_1_id"] = away_ids_names[0][0]
            period_df.loc[period_df.index[i], "away_player_1"] = away_ids_names[0][1]
            period_df.loc[period_df.index[i], "away_player_2_id"] = away_ids_names[1][0]
            period_df.loc[period_df.index[i], "away_player_2"] = away_ids_names[1][1]
            period_df.loc[period_df.index[i], "away_player_3_id"] = away_ids_names[2][0]
            period_df.loc[period_df.index[i], "away_player_3"] = away_ids_names[2][1]
            period_df.loc[period_df.index[i], "away_player_4_id"] = away_ids_names[3][0]
            period_df.loc[period_df.index[i], "away_player_4"] = away_ids_names[3][1]
            period_df.loc[period_df.index[i], "away_player_5_id"] = away_ids_names[4][0]
            period_df.loc[period_df.index[i], "away_player_5"] = away_ids_names[4][1]
        elif (
            period_df.iloc[i, :]["event_type_de"] == "substitution"
            and pd.isnull(period_df.iloc[i, :]["homedescription"]) == 1
        ):
            away_ids_names = [
                ids
                for ids in away_ids_names
                if ids[0] != period_df.iloc[i, :]["player1_id"]
            ]
            away_ids_names.append((period_df.iloc[i, 20], period_df.iloc[i, 21]))
            period_df.loc[period_df.index[i], "home_player_1_id"] = home_ids_names[0][0]
            period_df.loc[period_df.index[i], "home_player_1"] = home_ids_names[0][1]
            period_df.loc[period_df.index[i], "home_player_2_id"] = home_ids_names[1][0]
            period_df.loc[period_df.index[i], "home_player_2"] = home_ids_names[1][1]
            period_df.loc[period_df.index[i], "home_player_3_id"] = home_ids_names[2][0]
            period_df.loc[period_df.index[i], "home_player_3"] = home_ids_names[2][1]
            period_df.loc[period_df.index[i], "home_player_4_id"] = home_ids_names[3][0]
            period_df.loc[period_df.index[i], "home_player_4"] = home_ids_names[3][1]
            period_df.loc[period_df.index[i], "home_player_5_id"] = home_ids_names[4][0]
            period_df.loc[period_df.index[i], "home_player_5"] = home_ids_names[4][1]
            period_df.loc[period_df.index[i], "away_player_1_id"] = away_ids_names[0][0]
            period_df.loc[period_df.index[i], "away_player_1"] = away_ids_names[0][1]
            period_df.loc[period_df.index[i], "away_player_2_id"] = away_ids_names[1][0]
            period_df.loc[period_df.index[i], "away_player_2"] = away_ids_names[1][1]
            period_df.loc[period_df.index[i], "away_player_3_id"] = away_ids_names[2][0]
            period_df.loc[period_df.index[i], "away_player_3"] = away_ids_names[2][1]
            period_df.loc[period_df.index[i], "away_player_4_id"] = away_ids_names[3][0]
            period_df.loc[period_df.index[i], "away_player_4"] = away_ids_names[3][1]
            period_df.loc[period_df.index[i], "away_player_5_id"] = away_ids_names[4][0]
            period_df.loc[period_df.index[i], "away_player_5"] = away_ids_names[4][1]
        else:
            period_df.loc[period_df.index[i], "home_player_1_id"] = home_ids_names[0][0]
            period_df.loc[period_df.index[i], "home_player_1"] = home_ids_names[0][1]
            period_df.loc[period_df.index[i], "home_player_2_id"] = home_ids_names[1][0]
            period_df.loc[period_df.index[i], "home_player_2"] = home_ids_names[1][1]
            period_df.loc[period_df.index[i], "home_player_3_id"] = home_ids_names[2][0]
            period_df.loc[period_df.index[i], "home_player_3"] = home_ids_names[2][1]
            period_df.loc[period_df.index[i], "home_player_4_id"] = home_ids_names[3][0]
            period_df.loc[period_df.index[i], "home_player_4"] = home_ids_names[3][1]
            period_df.loc[period_df.index[i], "home_player_5_id"] = home_ids_names[4][0]
            period_df.loc[period_df.index[i], "home_player_5"] = home_ids_names[4][1]
            period_df.loc[period_df.index[i], "away_player_1_id"] = away_ids_names[0][0]
            period_df.loc[period_df.index[i], "away_player_1"] = away_ids_names[0][1]
            period_df.loc[period_df.index[i], "away_player_2_id"] = away_ids_names[1][0]
            period_df.loc[period_df.index[i], "away_player_2"] = away_ids_names[1][1]
            period_df.loc[period_df.index[i], "away_player_3_id"] = away_ids_names[2][0]
            period_df.loc[period_df.index[i], "away_player_3"] = away_ids_names[2][1]
            period_df.loc[period_df.index[i], "away_player_4_id"] = away_ids_names[3][0]
            period_df.loc[period_df.index[i], "away_player_4"] = away_ids_names[3][1]
            period_df.loc[period_df.index[i], "away_player_5_id"] = away_ids_names[4][0]
            period_df.loc[period_df.index[i], "away_player_5"] = away_ids_names[4][1]

    return period_df


def main_scrape(game_id):
    """
    this is the main function that runs and ties all them together. Doing it
    this way so I can better write tests that work on Travis CI due to their
    IP being blacklisted by NBA.com.

    Inputs:
    game_id     - NBA game id of game to be scraped

    Outputs:
    game_df     - pandas dataframe of the play by play
    """

    v2_dict = get_pbp_api(game_id)
    game_df = scrape_pbp(v2_dict)
    periods = []
    if game_id == "0021500916":
        game_df = game_df[game_df["period"] < 5]
    for period in range(1, game_df["period"].max() + 1):
        lineups = get_lineup_api(game_id, period)
        periods.append(
            get_lineup(game_df[game_df["period"] == period].copy(), lineups, game_df,)
        )
    game_df = pd.concat(periods).reset_index(drop=True)
    season_dict = {
        "1": "Pre+Season",
        "2": "Regular+Season",
        "3": "All+Star",
        "4": "Playoffs",
        "5": "Regular+Season",
    }
    season_type = season_dict[game_df["game_id"].unique()[0][2:3]]
    if game_df["game_id"].unique()[0][2:3] == "5":
        game_df["game_date"] = "2020-08-15"
    else:
        if game_df["game_id"].unique()[0][3:5] == "99":
            season = "1999-00"
        else:
            season = f"20{game_df['game_id'].unique()[0][3:5]}-{int(game_df['game_id'].unique()[0][3:5]) + 1}"
        date_url = (
            f"https://stats.nba.com/stats/teamgamelog?DateFrom=&DateTo=&LeagueID=&"
            f"Season={season}"
            f"&SeasonType={season_type}&TeamID={game_df['home_team_id'].unique()[0]}"
        )
        dates = requests.get(date_url, headers=USER_AGENT)
        dates_dict = json.loads(dates.text)
        schedule = dates_dict["resultSets"][0]["rowSet"]
        game_date = [g[2] for g in schedule if g[1] == game_df["game_id"].unique()[0]]
        formatted_date = datetime.datetime.strptime(game_date[0], "%b %d, %Y")
        game_df["game_date"] = formatted_date

    return game_df
