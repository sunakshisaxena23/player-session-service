import pandas as pd
from datetime import datetime, timezone, timedelta


def get_start_events_by_country(num_of_hours, db):
    db._db_cur.row_factory = pandas_factory
    db._db_cur.default_fetch_size = None

    yymmddhh = get_year_month_hour(int(num_of_hours))

    query = "select country,sum(session_count) from players.start_events_by_hour" \
            " where YYMMDDHH >={}  ALLOW FILTERING ".format(yymmddhh)
    result = db._db_cur.execute(query, timeout=None)._current_rows

    df = result.groupby('country').sum()

    res = {}

    if df.empty:
        res['error'] = "No start events for the input time"

    else:
        res['count'] = df.to_json()

    return res


def get_session_by_player_id(player_id, db):
    db._db_cur.row_factory = pandas_factory
    db._db_cur.default_fetch_size = None

    query = "SELECT session_id,end_timestamp FROM players.events_session_by_player_id WHERE player_id = '{}' \
             and is_ended= true and is_started= true ALLOW FILTERING;" \
        .format(player_id)
    result = db._db_cur.execute(query, timeout=None)._current_rows
    df = result.sort_values(by=['end_timestamp'], ascending=False).head(20)

    res = {}

    if df.empty:
        res['error'] = "No events for player_id: " + player_id

    else:
        res['session_ids'] = df.to_json()

    return res


def get_year_month_hour(num_of_hours):

    d = datetime.now(timezone.utc) - timedelta(hours=num_of_hours)

    return d.strftime('%Y%m%d%H')


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)





