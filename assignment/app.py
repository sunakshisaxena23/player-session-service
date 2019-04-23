import ndjson
from assignment.util import json_api, ndjson_api
from flask import request, Flask, abort, json
from assignment.dao.cassandra_writer import process_events
from assignment.dao.cassandra_reader import get_start_events_by_country, get_session_by_player_id
from flask_batch import add_batch_route
from assignment.dao.cassandra_connection import CassandraConnection

import logging


app = Flask(__name__)
add_batch_route(app)

file_handler = logging.FileHandler('app.log')
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)

db = CassandraConnection()
db.create_connection("127.0.0.1", "players")

HEADERS = {"Content-Type": "application/json"}


@app.route('/', defaults={"path": ""})
@app.route('/<path:path>')
def index(path=None):
    return "Events Tracking is active"


@app.route('/batchevents', methods=['POST'])
@ndjson_api
def add_event():

        try:
            requests = ndjson.loads(request.data.decode('utf-8'))
            return process_events(requests, db)

        except ValueError as e:
            abort(400, e)


@app.route('/queryevents/count', methods=['POST'])
@json_api
def get_count_of_session_by_country():
    try:
        data = json.loads(request.data)
        num_of_hours = data.get('num_of_hours', None)

        if num_of_hours is None:
            return {"error": "Invalid Input"}
        else:
            return get_start_events_by_country(num_of_hours, db)

    except ValueError as e:
        abort(400, e)


@app.route('/queryevents/topn', methods=['POST'])
@json_api
def get_top_session_by_player():

    try:
        data = json.loads(request.data)
        player_id = data.get('player_id', None)
        return get_session_by_player_id(player_id, db)

    except ValueError as e:
        abort(400, e)


if __name__ == "__main__":
    app.logger.info('starting player session service')
    app.run(debug=True, threaded=True)
