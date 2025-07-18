# this will catalog the shot types recorded in the NBA play by play
# not sure how accurate this is it seems to change for the same shots
# I think I have them all added but could be wrong.
# TODO get all the shot types from the hackathon data they sent out and update
# this dictionary
import datetime
import requests

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": requests.utils.default_user_agent(),
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nba.com",
    }
)


# this dictionary will categorize the event types that happen in the NBA
# play by play
EVENT_TYPE_DICT = {
    1: "shot",
    2: "missed_shot",
    4: "rebound",
    5: "turnover",
    20: "stoppage: out-of-bounds",
    6: "foul",
    3: "free-throw",
    8: "substitution",
    12: "period-start",
    10: "jump-ball",
    9: "team-timeout",
    18: "instant-replay",
    13: "period-end",
    7: "goal-tending",
    0: "game-end",
}


def get_date_games(from_date, to_date):
    """Return all game ids between two dates."""
    game_ids = []
    cur_date = datetime.datetime.strptime(from_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(to_date, "%Y-%m-%d")

    while cur_date <= end:
        url = (
            "https://cdn.nba.com/static/json/liveData/scoreboard/"
            f"scoreboard_{cur_date.strftime('%Y%m%d')}.json"
        )
        data = SESSION.get(url).json()
        for game in data.get("scoreboard", {}).get("games", []):
            game_ids.append(game.get("gameId"))
        cur_date += datetime.timedelta(days=1)

    return game_ids


def get_season(date):
    """
    Get Season based on date

    Inputs:
    date  -  time_struct of date

    Outputs:
    season - e.g. 2018
    """
    year = date.year
    if date.month >= 9:
        return year
    return year - 1
