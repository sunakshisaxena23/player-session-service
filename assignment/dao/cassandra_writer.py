from cassandra.query import BatchStatement, ConsistencyLevel
from flask import abort
from datetime import datetime


def process_events(event_data, db):

    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    insert_count_activity_by_date = db._db_cur.prepare(
        "UPDATE players.start_events_by_hour SET session_count = session_count + 1 where YYMMDDHH = ? and country = ? ")

    insert_activity_by_user = db._db_cur.prepare("UPDATE players.events_session_by_player_id \
        SET is_started = ? , start_timestamp = ? \
        where player_id = ? AND session_id = ?")

    update_event = db._db_cur.prepare("UPDATE players.events_session_by_player_id \
                    SET is_ended = ? , end_timestamp = ? \
                    where player_id = ? AND session_id = ?")
    for i in event_data:

        event = i['event']
        player_id = i['player_id']
        session_id = i['session_id']
        """parse string to date time format example ts : 2016-12-02T12:48:05 """
        dtime = datetime.strptime(i['ts'], '%Y-%m-%dT%H:%M:%S')

        if event == 'start':
            yymmddhh = int(dtime.strftime('%Y%m%d%H'))
            country = i['country']
            is_started = True
            start_timestamp = dtime

            db._db_cur.execute(insert_count_activity_by_date, (yymmddhh, country))
            batch.add(insert_activity_by_user,
                      (is_started, start_timestamp, player_id, session_id))

        elif event == 'end':
            is_ended = True
            end_timestamp = dtime
            batch.add(update_event, (is_ended, end_timestamp, player_id, session_id))
            print(dtime)

        else:
            abort(400, 'Incorrect event type')

    if len(batch) != 0:
        db._db_cur.execute(batch)






