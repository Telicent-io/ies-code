#LICENSE = GPL3 (Gnu Public License v3)
#Produced under contract to Dstl 

from rdflib import Graph, plugin, URIRef, BNode, Literal
from rdflib.namespace import DC, DCAT, DCTERMS, OWL, RDF, RDFS, XMLNS, XSD
from rdflib.serializer import Serializer
import uuid
import dateutil.parser #required to do earliest, latest date time comparison
import ies_functions as ies
import sqlite3
from time import sleep
import geohash_hilbert as ghh

aisDataFile="./2ferries4weeks.csv"
aisDB="../GeoData/nyc.db" #This file can be downloaded from https://ais-sqlite-db.s3.eu-west-2.amazonaws.com/nyc.db - change your path as necessary
pingDelay = 0.1 #forced delay between reading off individual IES pings
sampleSize=20 #The total number of pings to process
trackSize=4 #The number of location pings to accrue before pushing out a track

#Set this to True to use Kafka
useKafka = False

if useKafka:
    iesOutput = "kafka"
    kafkaBroker = ies.initialiseKafka(kHost="localhost:9092") #change this if Kafka is not on local machine
    iesKafkaTopic = "ies"
else:
    iesOutput = "file"


#Simple function to process a line of AIS in IES. It expects mmsi, timestamp (in ISO8601 format), lat, lon
def createLocationObservation(iesGraph,ping,obs=None,transponder=None,measures=None):
    mmsi = str(ping[0])
    timestamp = str(ping[1])
    lat = float(ping[2])
    lon = float(ping[3])
    print(lat,lon)
    if transponder == None:
        transponder = ies.createLocationTransponder(iesGraph=iesGraph,mmsi=mmsi)
    lo = ies.instantiate(iesGraph=iesGraph,_class=ies.locationObservation)
    #If track emulation is not required, obs will be None. If it's not None, make the LocationObservation (lo) part of the overall track observation
    if obs:
        ies.addToGraph(iesGraph=iesGraph,subject=lo,predicate=ies.ipao,obj=obs)
    #...and the ParticularPeriod in which the observation occurred
    ies.putInPeriod(iesGraph=iesGraph,item=lo,timeString=timestamp)
    #And involve the transponder in that location observation
    ltPart = ies.instantiate(iesGraph=iesGraph,_class=ies.observedTarget)
    ies.addToGraph(iesGraph=iesGraph,subject=ltPart,predicate=ies.ipo,obj=transponder) #participation of the transponder
    ies.addToGraph(iesGraph=iesGraph,subject=ltPart,predicate=ies.ipi,obj=lo) #participation in the LocationObservation
    #Now the observed location, a geopoint with a lat and long - using a geohash to give each point a unique uri
    gp = URIRef(ies.dataUri+"latlong"+str(lat)+","+str(lon))
    ies.instantiate(iesGraph=iesGraph,_class=ies.geoPoint,instance=gp)
    #Add the lat and long values as identifiers of the geopoint...firstly creating repeatable URIs for them so they don't overwrite
    latObj = URIRef(gp.toPython()+"_lat")
    lonObj = URIRef(gp.toPython()+"_lon")
    ies.instantiate(iesGraph=iesGraph, _class=ies.latitude,instance=latObj)
    ies.instantiate(iesGraph=iesGraph, _class=ies.longitude,instance=lonObj)
    ies.addToGraph(iesGraph=iesGraph,subject=gp,predicate=ies.iib,obj=latObj)
    ies.addToGraph(iesGraph=iesGraph,subject=gp,predicate=ies.iib,obj=lonObj)
    #Add the representation values to the lat and lon objects
    ies.addToGraph(iesGraph=iesGraph,subject=latObj,predicate=ies.rv,obj=Literal(lat, datatype=XSD.string))
    ies.addToGraph(iesGraph=iesGraph,subject=lonObj,predicate=ies.rv,obj=Literal(lon, datatype=XSD.string))
    #Now the participation of the GeoPoint in the Observation
    gpPart = ies.instantiate(iesGraph=iesGraph,_class=ies.observedLocation)
    ies.addToGraph(iesGraph=iesGraph,subject=gpPart,predicate=ies.ipo,obj=gp) #participation of the GeoPoint
    ies.addToGraph(iesGraph=iesGraph,subject=gpPart,predicate=ies.ipi,obj=lo) #participation in the LocationObservation
    if measures:
        sogVal = float(ping[4])
        cogVal = float(ping[5])
        sog = ies.addMeasure(iesGraph=iesGraph,measureClass=measures["sogClass"],value=sogVal,uom=measures["knots"])
        cog = ies.addMeasure(iesGraph=iesGraph,measureClass=measures["cogClass"],value=sogVal,uom=measures["degTN"])
        ies.addToGraph(iesGraph=iesGraph,subject=ltPart,predicate=ies.och,obj=sog)
        ies.addToGraph(iesGraph=iesGraph,subject=ltPart,predicate=ies.och,obj=cog)



#A simple parent observation to group the others into track
def createParentObservation(iesGraph):
    return ies.instantiate(iesGraph=iesGraph,_class=ies.observation)

def exportTrack(iesGraph,track,output="file"):
    #The track dictionary should already have min and max timestamps for the pings it contains
    ies.initialiseGraph(iesGraph=iesGraph)
    #Add a parent observation
    obs = ies.instantiate(iesGraph=iesGraph,_class=ies.observation)
    ies.startsIn(iesGraph=iesGraph,item=obs,timeString=track["minDateTime"].isoformat())
    ies.endsIn(iesGraph=iesGraph,item=obs,timeString=track["maxDateTime"].isoformat())
    measures = dict({})
    #Now add the measure classes for Speed Over Ground and Course Over Ground...and knots for the Unit Of Measure
    measures["sogClass"] = ies.instantiate(iesGraph=iesGraph,_class=ies.classOfMeasure,instance=URIRef(ies.dataUri+"SpeedOverGround"))
    measures["cogClass"] = ies.instantiate(iesGraph=iesGraph,_class=ies.classOfMeasure,instance=URIRef(ies.dataUri+"CourseOverGround"))
    measures["knots"] = ies.instantiate(iesGraph=iesGraph,_class=ies.unitOfMeasure,instance=URIRef(ies.dataUri+"Knots"))
    measures["degTN"] = ies.instantiate(iesGraph=iesGraph,_class=ies.unitOfMeasure,instance=URIRef(ies.dataUri+"DegreesTrueNorth"))
    ies.addName(iesGraph=iesGraph,item=measures["sogClass"],nameString="Speed Over Ground")
    ies.addName(iesGraph=iesGraph,item=measures["cogClass"],nameString="Course Over Ground")
    ies.addName(iesGraph=iesGraph,item=measures["knots"],nameString="knots")
    ies.addName(iesGraph=iesGraph,item=measures["degTN"],nameString="degrees true North")
    #add the location transponder - We don't know this is necessarily a vessel. All we know is that we have a LocationTransponder. 
    lt = ies.createLocationTransponder(iesGraph=iesGraph,mmsi=track["id"])
    obsvd = ies.instantiate(iesGraph=iesGraph,_class=ies.observedTarget)
    ies.addToGraph(iesGraph=iesGraph,subject=obsvd,predicate=ies.ipo,obj=lt)
    ies.addToGraph(iesGraph=iesGraph,subject=obsvd,predicate=ies.ipi,obj=obs)
    #now go through the individual location observations and add those...
    for ping in track["pings"]:
        createLocationObservation(iesGraph=iesGraph,ping=ping,transponder=lt,obs=obs,measures=measures) 
    if output == "kafka":
        ies.sendToKafka(iesGraph=iesGraph,kProducer=kafkaBroker,kTopic=iesKafkaTopic)
    else:
        ies.saveRdf(iesGraph,'./data/track'+track["id"]+'-'+str(track["counter"])+'.ies.ttl')

#This runs through a sqlite3 database of IES data (see download instructions at start of this file) and creates tracks
#If output is set to "file" they're exported as turtle files, indexed by track number and mmsi
#If output is set to "kafka" they're exported to kafka (see global variables at top of file for Kafka settings)
def processDatabase(dbFileName,delay,batchSize,output="file"):
    trackDict ={} #This is used to keep a record of the number of pings agains each vessel
    conn = sqlite3.connect(dbFileName)
    cursor = conn.cursor()
    cursor.execute("""select * from position ORDER BY BaseDateTime""")

    for row in cursor.fetchall():
        mmsi = row[0]
        dt = dateutil.parser.parse(row[1])
        if mmsi in trackDict:
            trackDict[mmsi]["pings"].append(row)
            if trackDict[mmsi]["maxDateTime"] == None or dt > trackDict[mmsi]["maxDateTime"]:
                trackDict[mmsi]["maxDateTime"] = dt
            if trackDict[mmsi]["minDateTime"] == None or dt < trackDict[mmsi]["minDateTime"]:
                trackDict[mmsi]["minDateTime"] = dt
                
            if len(trackDict[mmsi]["pings"]) >= batchSize:
                #batch size for track has been reached, time to export a track
                exportTrack(graph,trackDict[mmsi],output)
                trackDict[mmsi]["minDateTime"] = None
                trackDict[mmsi]["maxDateTime"] = None
                trackDict[mmsi]["pings"] = []
                trackDict[mmsi]["counter"] = trackDict[mmsi]["counter"] + 1
        else:
            trackDict[mmsi] = {"id":mmsi,"pings":[row],"minDateTime":dt,"maxDateTime":dt,"counter":0}
        sleep(delay)

#Set up the rdf graph
graph=ies.initialiseGraph(None)
#Now process the data in the database (each row comes out as a tuple)
#   0       1               2       3       4       5       6           7           8       9           10          11      12      13      14      15
    #  MMSI,    BaseDateTime,   LAT,    LON,    SOG,    COG,    Heading,    VesselName, IMO,    CallSign,   VesselType, Status, Length, Width, Draft,   Cargo
#The important thing here is whether or not you use Kafka. Change the global useKafka variable at the top of the script to change this. 
processDatabase(dbFileName=aisDB,delay=pingDelay,batchSize=trackSize,output=iesOutput)

    
