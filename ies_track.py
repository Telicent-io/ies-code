#LICENSE = GPL3 (Gnu Public License v3)
#Produced under contract to Dstl 

from rdflib import Graph, plugin, URIRef, BNode, Literal
from rdflib.namespace import DC, DCAT, DCTERMS, OWL, RDF, RDFS, XMLNS, XSD
from rdflib.serializer import Serializer
import uuid
import dateutil.parser #required to do earliest, latest date time comparison
import Geohash   # (pip install Geohash but also pip install python-geohash )
import ies_functions as ies
import pandas as pd #Using PANDAS as its CSV import is reliable, and the native Python one is absolutely terrible. 
import sqlite3

aisDataFile="./2ferries4weeks.csv"
aisDB="../GeoData/nyc.db"
pingDelay = 1 #forced delay between reading off individual IES pings
sampleSize=20 #The total number of pings to process
trackSize=4 #The number of location pings to accrue before pushing out a track


#Simple function to process a line of AIS in IES. It expects mmsi, timestamp (in ISO8601 format), lat, lon
def createLocationObservation(iesGraph,mmsi,timestamp,lat,lon,obs=None):
    #add the location transponder - We don't know this is necessarily a vessel. All we know is that we have a LocationTransponder. 
    lt = ies.createLocationTransponder(iesGraph=iesGraph,mmsi=mmsi)
    #Now create the observation event
    lo = ies.instantiate(iesGraph=iesGraph,_class=ies.locationObservation)
    #If track emulation is not required, obs will be None. If it's not None, make the LocationObservation (lo) part of the overall track observation
    if obs:
        ies.addToGraph(iesGraph=iesGraph,subject=lo,predicate=ies.ipao,obj=obs)
    #...and the ParticularPeriod in which the observation occurred
    ies.putInPeriod(iesGraph=iesGraph,item=lo,iso8601TimeString=timestamp)
    #And involve the transponder in that location observation
    ltPart = ies.instantiate(iesGraph=iesGraph,_class=ies.observedTarget)
    ies.addToGraph(iesGraph=iesGraph,subject=ltPart,predicate=ies.ipo,obj=lt) #participation of the transponder
    ies.addToGraph(iesGraph=iesGraph,subject=ltPart,predicate=ies.ipi,obj=lo) #participation in the LocationObservation
    #Now the observed location, a geopoint with a lat and long - using a geohash to give each point a unique uri
    gp = URIRef(ies.dataUri+"geohash_"+Geohash.encode(lat,lon))
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


#A simple parent observation to group the others into track
def createParentObservation(iesGraph):
    return ies.instantiate(iesGraph=iesGraph,_class=ies.observation)

def exportTrack(iesGraph,track):
    #The track dictionary should already have min and max timestamps for the pings it contains
    print(track["id"],track["minDateTime"],track["maxDateTime"])

#This runs through a sqlite3 database of IES data (see download instructions at start of this file) and creates tracks
def processDatabase(dbFileName,delay,batchSize):
    trackDict ={} #This is used to keep a record of the number of pings agains each vessel
    conn = sqlite3.connect(dbFileName)
    cursor = conn.cursor()
    data = cursor.execute("""
    select * from position
    """)

    for row in cursor.fetchall():
        mmsi = row[0]
        dt = dateutil.parser.parse(row[1])
        if mmsi in trackDict:
            print(trackDict[mmsi])
            trackDict[mmsi]["pings"].append(row)
            if dt > trackDict[mmsi]["maxDateTime"]:
                trackDict[mmsi]["maxDateTime"] = dt
            if dt < trackDict[mmsi]["minDateTime"]:
                trackDict[mmsi]["minDateTime"] = dt
                

            print(mmsi,len(trackDict[mmsi]["pings"]))
            print(trackDict)
            if len(trackDict[mmsi]["pings"]) >= batchSize:
                #batch size for track has been reached, time to export a track
                exportTrack(graph,trackDict[mmsi])
                trackDict[mmsi]["pings"] = []

        else:
            trackDict[mmsi] = {"id":mmsi,"pings":[row],"minDateTime":dt,"maxDateTime":dt}


#Set up the rdf graph
graph=ies.initialiseGraph()

processDatabase(dbFileName=aisDB,delay=pingDelay,batchSize=trackSize)
quit()




#Load the raw AIS data
#The messages are arrays of AIS fields:
    #   0       1               2       3       4       5       6           7           8       9           10          11      12      13      14      15
    #  MMSI,    BaseDateTime,   LAT,    LON,    SOG,    COG,    Heading,    VesselName, IMO,    CallSign,   VesselType, Status, Length, Width, Draft,   Cargo
aisDataset = pd.read_csv(aisDataFile)

#Extract the smaller sample...
sample = aisDataset.head(sampleSize)
print(sample)
quit()


#Add a parent observation
obs = createTrack(graph) #comment this line out to prevent a track being created

#First pass through the 


#run the positions data through
earliestTimeStamp = None
latestTimeStamp = None
#trackDict is used to separate out the 
for aisLine in ais:
    #check to see if this datetime is earlier or later than previous ones
    if earliestTimeStamp == None:
        earliestTimeStamp = dateutil.parser.parse(aisLine[1])
    else:
        newTime = dateutil.parser.parse(aisLine[1])
        if newTime < earliestTimeStamp:
            earliestTimeStamp = newTime
    if latestTimeStamp == None:
        latestTimeStamp = dateutil.parser.parse(aisLine[1])
    else:
        newTime = dateutil.parser.parse(aisLine[1])
        if newTime > latestTimeStamp:
            latestTimeStamp = newTime
    createLocationObservation(iesGraph=graph,mmsi=aisLine[0],timestamp=aisLine[1],lat=aisLine[2],lon=aisLine[3],obs=obs)
print("earliest:",earliestTimeStamp.isoformat())
print("latest",latestTimeStamp.isoformat())
#Now we use the earliest and latest to timebox the observation itself
ies.startsIn(iesGraph=graph,item=obs,iso8601TimeString=earliestTimeStamp.isoformat())
ies.endsIn(iesGraph=graph,item=obs,iso8601TimeString=latestTimeStamp.isoformat())

ies.saveRdf(graph,'track.ies.ttl')