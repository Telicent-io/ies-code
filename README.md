# ies-code

Simple repo of shareable (GPL) IES code for getting started with IES. We'll put more of these up over time.

Code samples are:

* ies_following.py - [NOT IMPLEMENTED YET] takes a batch of AIS pings and simulates a track of observed geo points in IES RDF
* ies_track.py - runs through a large(ish) database of AIS pings and either dumps them to files or to Kafka. You can download the AIS database from https://ais-sqlite-db.s3.eu-west-2.amazonaws.com/nyc.db or there is a much larger database at https://ais-sqlite-db.s3.eu-west-2.amazonaws.com/fifth.db.zip which has a whole days worth of north american data. Both datasets originated from https://marinecadastre.gov/ais/ where no license information is given so assumed to be open source
* ies_functions.py - used by the previous two scripts to produce IES instances
* kafka-monitor (folder) - dash web app for monitoring RDF throughput on KAFKA
* k-stream-ais - used in phase 1 of MIKES because Reply wasn't available to us initially. This script streams AIS messages to Kafka. It reads those messages from the sqlite databases mentioned above. The batch size and rate can be altered in the code. Things like Kafka IP and topic name are set as env variables to enable easier containerisation

The original scripts that were posted as a teaching exercise are in the GettingStarted folder - these are no longer maintained

The extensions to IES required for the AIS work are in the Schema Extensions folder, along with a powerpoint and the original IES 4.2 schema. 

