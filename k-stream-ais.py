from kafka import KafkaProducer
from time import sleep
from json import dumps
import csv
from io import StringIO
import os
import sys
import sqlite3
import time
import zlib
from threading import Thread
from flask import Flask
from flask_restful import Resource, Api

#reminder about text colour codes...
# \033[0;31m red
# \033[0;33m yellow
# \033[0;32m green

#Tom - you maybe want to make this ENV variables ?  Not sure what your criteria for doing this was.
#Set the delay between messages (in seconds)
msgDelay = 0.1
#number of messages to process
msgSize = 8
status="STOPPED"
msgFormat="DSTL" 




broker = os.getenv("BOOTSTRAP_SERVERS")
aisTopic = os.getenv("AIS_TOPIC")
db_filename = os.getenv("AIS_DATASOURCE") #this is the sqlite .db file (e.g. fifth.db)
#connect to the Kafka instance - you will need to change this for remote server / cloud shizzle
KAFKA_VERSION=(2,5)
producer = KafkaProducer(bootstrap_servers=[broker], api_version=KAFKA_VERSION)

if db_filename is None or broker is None or aisTopic is None:
    sys.exit("BOOTSTRAP_SERVERS, AIS_DATASOURCE and AIS_TOPIC must all be set as env variables")

#connect to the database
conn = sqlite3.connect(db_filename)



#This is a simple script to drip-feed AIS messages onto the "ais" topic on Kafka. 
#The messages are arrays of AIS fields:
    #   0       1               2       3       4       5       6           7           8       9           10          11      12      13      14      15
    #  MMSI,    BaseDateTime,   LAT,    LON,    SOG,    COG,    Heading,    VesselName, IMO,    CallSign,   VesselType, Status, Length, Width, Draft,   Cargo

#Each message is is first dumped to text (JSON), encoded as a utf-8 byte string, then zipped
#The reason for encoding as JSON rather than dumping a binary Python object is so that the message can be read in other dev languages









def processTable(dbFile,kProducer):
    global msgSize
    global status
    global msgDelay
    conn = sqlite3.connect(dbFile)
    cursor = conn.cursor()
    cursor.execute("SELECT Count() FROM position")
    numberOfRows = cursor.fetchone()[0]
    #Now query them in timestamp order
    print("Sorting records by timestamp")
    cursor.execute("""select * from position ORDER BY BaseDateTime""")
    print("processing message batches...")
    print("\033[93mTotal number of AIS records:",numberOfRows)
    for i in range (round(numberOfRows / msgSize)):
        if status == "STOPPED":
            print("Stream stopped")
            break
        startTime = time.perf_counter()
        packet = cursor.fetchmany(msgSize)
        
        #DSTL format...
         #   0       1               2       3       4       5       6           7           8       9           10          11      12      13      14      15
        #  MMSI,    BaseDateTime,   LAT,    LON,    SOG,    COG,    Heading,    VesselName, IMO,    CallSign,   VesselType, Status, Length, Width, Draft,   Cargo

        #OZ format...
       # 0               1           2             3    4       5       6           7       8       9           10
        #CRAFT_ID,      LON,        LAT,        COURSE, SPEED,  TYPE,   SUBTYPE,    LENGTH, BEAM,   DRAUGHT,    TIMESTAMP
        #503444444,     142.99254,  -42.005783, 0,      0,      30,        ,            ,       ,          ,   01/04/2020 12:00:00 AM
        #370123456,     143.5639,   -42.073288, 90,     20,     30,         ,           ,       ,           ,   01/04/2020 12:00:00 AM\
        


        if msgFormat == "OZ": 
            stringMessage = ""
            for msg in packet:
                ozMsg = [msg[0],msg[3],msg[2],msg[5],msg[4],msg[10],None,msg[12],msg[13],msg[14],msg[1],msg[7],msg[8],msg[9]]
                line = StringIO()
                writer = csv.writer(line)
                writer.writerow(ozMsg)
                stringMessage = stringMessage + line.getvalue()
            kProducer.send(aisTopic, stringMessage.encode())
        else:
            msgList = []
            for msg in packet:
                msgList.append(list(msg))
            kProducer.send(aisTopic, value=zlib.compress(bytes(dumps(msgList),"utf-8")))
        if commandLine:
            print("\033[0;32m--batch number:",i,"batch size:",len(packet),"processing time:",time.perf_counter()-startTime)
        #print(msgList)
        sleep(msgDelay)

aisThread = Thread( target=processTable, args=(db_filename,producer) )
#aisThread.start()
#aisThread.join()

def create_app():
    app = Flask(__name__)

    def interrupt():
        global aisThread
        aisThread.cancel()

    return app

app = create_app()    
api = Api(app)

class HelloWorld(Resource):
    def get(self):
        return {'status': 'WAITING'}

class StartAIS(Resource):
    def get(self):
        global status
        status = "STARTED"
        aisThread.start()
        return {'status': 'STARTED'}

class StopAIS(Resource):
    def get(self):
        global status
        status="STOPPED"
        return {'status': 'STOPPED'}

class Batch(Resource):
    def get(self, batch_size):
        global msgSize
        msgSize = batch_size #delete me !
        return {"messageSize":msgSize}

    def put(self, batch_size):
        global msgSize
        msgSize = batch_size 
        print("message size set to: "+str(msgSize))
        return {"messageSize":msgSize}

class Delay(Resource):
    def get(self, dly):
        global msgDelay
        msgDelay = dly #delete me !
        return {"messageDelay":msgDelay}

    def put(self, dly):
        global msgDelay
        msgDelay = dly 
        print("message delay set to: "+str(msgDelay))
        return {"messageDelay":msgDelay}


if len(sys.argv) > 1:
    print("\033[94mTelicent AIS Streamer - built for Dstl under DEFCON705 - Copyright Telicent Ltd 2020")
    print("\033[95mMessage Batch size:",msgSize)
    print("Delay between messages:",msgDelay," secs")

    if sys.argv[1] == "start":
        status = "STARTED"
        commandLine = True
        aisThread.start()
    if sys.argv[1] == "serve":
        commandLine = False
        api.add_resource(Batch, '/batch/<int:batch_size>')
        api.add_resource(Delay, '/delay/<float:dly>')
        api.add_resource(StartAIS, '/start')
        api.add_resource(StopAIS, '/stop')
        api.add_resource(HelloWorld, '/')
        if __name__ == '__main__':
            app.run(debug=True)
else:
    print("start with parameter 'serve' for REST API or 'start' to just run it - e.g. 'python k-strean-ais.py serve'")