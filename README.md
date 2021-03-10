# ies-code

Simple repo of shareable (GPL) IES code for getting started with IES. We'll put more of these up over time.

Code samples are:

* ies_following.py - [NOT IMPLEMENTED YET] takes a batch of AIS pings and simulates a track of observed geo points in IES RDF
* ies_track.py - runs through a large(ish) database of AIS pings and either dumps them to files or to Kafka. You can download the AIS database from https://ais-sqlite-db.s3.eu-west-2.amazonaws.com/nyc.db
* ies_functions.py - used by the previous two scripts to produce IES instances
