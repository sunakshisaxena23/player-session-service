Player-session-service uses flask and python to receive data in batches from three endpoint. Cassandra is used as database for this api.

First Endpoint expects new line delimited jsons for each event and can receive 1 to 10 records one time in each request.  also it implements two other endpoint

POST Command :

curl -H "Content-Type: text/plain" -X POST 'http://127.0.0.1:5000/batchevents' --data-binary "@assignment_data.jsonl"


Second Endpoint is to query session starts for the last X (X is defined by the user) hours for each country.

POST Command :

curl -H "Content-Type: application/json" -X POST 'http://127.0.0.1:5000/queryevents/count' -d { "num_of_hours": "24"}

example select query on cassandra for request to second Endpoint:  

select country,sum(session_count) from players.start_events_by_hour where YYMMDDHH >=19011123  ALLOW FILTERING;


Third Endpoint is for fetching last 20 complete sessions for a given player.

POST Command :

curl -H "Content-Type: application/json" -X POST 'http://127.0.0.1:5000/queryevents/topn' -d { "player_id": "0a2d12a1a7e145de8bae44c0c6e06629"}

example select query on cassandra for request to third Endpoint: 

SELECT session_id FROM players.events_session_by_player_id WHERE player_id = '1970-01-17 09:07:17+0100' and is_ended= true and is_started= true limit 20 ALLOW FILTERING;


Assumption made while building this api :

1. There is only one endpoint to receive start event and end event
2. ts field is in the utc timezone


Steps to install Cassandra on Mac OS :

brew install cassandra

run cassandra:

brew services start cassandra

commands to run , before running app in cassandra:

DDL:

CREATE KEYSPACE IF NOT EXISTS players WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 };

CREATE TABLE players.start_events_by_hour(
   YYMMDDHH int,
   country text,
   session_count counter,
   PRIMARY KEY ((YYMMDDHH),country)
   )

CREATE TABLE players.events_session_by_player_id(
   player_id text,
   is_started Boolean,
   is_ended Boolean,
   session_id text,s
   start_timestamp timestamp,
   end_timestamp timestamp,
   PRIMARY KEY (player_id,session_id))


Improvements:

Add Tests for this Python library.
Use config files for url , endpoint and DB connection details.

