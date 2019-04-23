Assumption :

There is only one endpoint to receive start event and end event
ts is in the utc timezone


Steps to install Cassandra on Mac OS :

brew install cassandra

run cassandra:

brew services start cassandra

commands to run , before running app in cassandra:

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



Query for retrival


API for fetching session starts for the last X (X is defined by the user) hours for each country

select country,sum(session_count) from players.start_events_by_hour where YYMMDDHH >=19011123  ALLOW FILTERING;

API for fetching last 20 complete sessions for a given player:

SELECT session_id FROM players.events_session_by_player_id WHERE player_id = '1970-01-17 09:07:17+0100' and is_ended= true and is_started= true limit 20 ALLOW FILTERING;


POST Command :





POST 'http://127.0.0.1:5000/queryevents/count' -d { "num_of_hours": "24"}

POST 'http://127.0.0.1:5000/queryevents/topn' -d { "player_id": "0a2d12a1a7e145de8bae44c0c6e06629"}


Result:

{"session_ids": "{\"session_id\":{\"0\":\"4a0c43c9-c43a-42ff-ba55-67563dfa35d4\"},\"end_timestamp\":{\"0\":1480682945520}}"}