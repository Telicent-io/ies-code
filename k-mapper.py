from kafka import KafkaConsumer, KafkaProducer
import urllib
from rdflib import Graph, plugin, URIRef, BNode, Literal
from rdflib.namespace import DC, DCAT, DCTERMS, OWL, RDF, RDFS, XMLNS, XSD
from rdflib.serializer import Serializer
from iesLib import IES, IESObject
import aisIesLib
from pyld import jsonld
import json
import time
import zlib
import urllib.parse
import os
import uuid
import pickledb

cache = dict()



broker = os.getenv("BOOTSTRAP_SERVERS")
iesTopic = os.getenv("IES_TOPIC")
aisTopic = os.getenv("DSTL_AIS_TOPIC")
dataNamespace = os.getenv("IES_NAMESPACE")

msgFormat="DSTL" 

namingSpaces = {}

#This script takes AIS messages from the "ais" topic in Kafka and maps them to IES and dumps to "ies" topic. Process is:
#Wait for Kafka message on "ais" topic
    #unzip the message
    #decode the UTF-8 binary
    #map the required fields to IES using the iesLib from Telicent
    #dump the IES message is n-triples
    #encode the n-triples text as UTF-8 binary string
    #zip the binary string
    #add the zipped object to the "ies" topic

#For MIKES Phase 2, this library is now reading "tracks" - i.e. bundles of sequential location pings in the format (see exampleTrack object below). Two major changes from Phase 1 of MIKES are:
  #This now maps to the position of the transponder, and the transponder has the MMSI - this avoids supposition around ship identity, and also avoids the mereological issues with very large ships being inside locations
  #The geopoint is now just part of the tolerence area in which the transponder is thought to be - this ensures the area of location is larger than the transponder

exampleTrack = {
  "metadata": {
    "mmsi": "367330510"
  },
  "states": [
    {
      "epsgCode": "4326",
      "timestamp": "2017-01-05T00:00:00",
      "coordinates": [
        "38.03654",
        "-122.12249"
      ]
    },
    {
      "epsgCode": "4326",
      "timestamp": "2017-01-05T00:00:00",
      "coordinates": [
        "38.03654",
        "-122.12249"
      ]
    },
    {
      "epsgCode": "4326",
      "timestamp": "2017-01-05T00:01:08",
      "coordinates": [
        "38.03654",
        "-122.12250"
      ]
    },
    {
      "epsgCode": "4326",
      "timestamp": "2017-01-05T00:02:09",
      "coordinates": [
        "38.03650",
        "-122.12249"
      ]
    },
    {
      "epsgCode": "4326",
      "timestamp": "2017-01-05T00:03:10",
      "coordinates": [
        "38.03653",
        "-122.12245"
      ]
    }
  ]
}

#Create an IES library, with a default data URI
ies = IES("./ies4_3.ttl",dataNamespace)


ies.mappings = {
    "ship":{
        "uriMap":{
           # "fieldName":"<<IDFIELDNAME>>",
            "mappingType":"GUID", #options are "FIELD" to map a value from a field/column, or "GUID" to generate an ID
            "uriBase":dataNamespace, #this is the prefix added to the ID (i.e. added ot the GUID or field value)
            "fieldID":8, #The index in the case of a CSV (zero-based)
            "missingID":"GUID" #when a FIELD option is selected and the value is blank or None - "SKIP" to not map it, "ERROR" to error out, #GUID to substitute a GUID
        },
        "classMap":{
            "fieldID":10,
            "classUriBase":"http://ies.data.gov.uk/ontology/ies4#",
            "extensionUriBase":"http://myOntology.org.uk/classes#",
            "defaultClasses":["VehicleState", "Observed" ], #These are used if no matches from below are found. Must be a valid IES classes !
            "defaultExtensions":[],
            "exactMatch":{
                "1001":{
                    "baseClasses":["VehicleState", "Observed" ],
                    "extensionClasses":["CommercialFishingVessel"]
                },
                "1002":{
                    "baseClasses":["VehicleState", "Observed" ],
                    "extensionClasses":["FishProcessingVessel"]
                },
                "1003":{
                    "baseClasses":["VehicleState", "Observed" ],
                    "extensionClasses":["FreightBarge"]
                },
                "1004":{
                    "baseClasses":["VehicleState", "Observed" ],
                    "extensionClasses":["FreightShip"]
                },
                "1005":{
                    "baseClasses":["VehicleState", "Observed" ],
                    "extensionClasses":["IndustrialVessel"]
                },
                "1007":{
                    "baseClasses":["VehicleState", "Observed" ],
                    "extensionClasses":["MobileOffshoreDrillingUnit"]
                },
                "1010":{
                    "baseClasses":["VehicleState", "Observed" ],
                    "extensionClasses":["OffshoreSupplyVessel"]
                },
                "1011":{
                    "baseClasses":"Ship",
                    "extensionClasses":["OilRecoveryVessel"]
                },
                "1012":{
                    "baseClasses":["VehicleState", "Observed" ],
                    "extensionClasses":["InspectedPassengerVessel"]
                },
                "1013":{
                    "baseClasses":["VehicleState", "Observed" ],
                    "extensionClasses":["UninspectedPassengerVessel"]
                }

            },
            "regexMatch":{
                "<<regex1>>":
                {

                },
                "<<regex2>>":
                {

                }
            }
        }
        
    }
}

def guid():
    return str(uuid.uuid4())

#Assume each message could contain more than one AIS message, and process each
def decodeAIS(message):
    for line in message:
        decodeAISLine(line)

def mapCSVLine(lineAsArray,mappingSpec):
    uri = ""
        
    if "uriMap" in mappingSpec:
        uriMap = mappingSpec["uriMap"]
        mmsi = lineAsArray[8]
        if not mmsi in cache:
            cache[mmsi] = {"lastAis":lineAsArray,"mmsiState":uriMap["uriBase"]+guid(),"classState":"","actState":""}

        if uriMap["mappingType"] == "GUID":
            uri = uriMap["uriBase"]+str(uuid.uuid4())
        elif uriMap["mappingType"] == "FIELD":
            if "fieldID" in uriMap:
                idVal = lineAsArray[uriMap["fieldID"]]
                if idVal == None or idVal == "":
                    if uriMap["missingID"] == "SKIP":
                        print("skipping item - no ID")                    
                        return None
                    elif uriMap["missingID"] == "GUID":
                        print("ID field missing, substituting a GUID")
                        uri = uriMap["uriBase"]+str(uuid.uuid4())
                    elif uriMap["missingID"] == "ERROR":
                        print(lineAsArray)
                        raise Exception("id field is missing in the data")
                else:
                    uri = uriMap["uriBase"]+idVal
                    #quit()
            else:
                raise Exception("no field number (fieldID) provided for ID - this is required in a CSV mapping")
            print(uri)
    else:
        raise Exception("No uriMap in mapping spec")
    #last check in case the URI didn't get set
    if uri == "":
        raise Exception("no URI could be set - check mapping script for errors")


    if "classMap" in mappingSpec:
        #The classMap dictionary is keyed by the exact values we want to match against. As dictionaries are indexed, this should run pretty fast
        #If performance is still not where we want it, we should consider a bloom filter here so we can quickly discard any non-matches
        classMap = mappingSpec["classMap"]
        sourceVal = lineAsArray[classMap["fieldID"]]
        classUriBase = classMap["classUriBase"]
        extUriBase = classMap["extensionUriBase"]
        targetClasses = []
        if sourceVal in classMap["exactMatch"]:
            for targetClass in classMap["exactMatch"][sourceVal]["baseClasses"]:
                targetClasses.append(classUriBase+targetClass)

            for extClass in classMap["exactMatch"][sourceVal]["extensionClasses"]:
                targetClasses.append(extUriBase+extClass)
        else:
            for regex in classMap["regexMatch"]:
                regexMap = classMap["regexMatch"][regex]
            #No exact matches so we need to trawl through the regex collection...bye bye performance !
        if targetClasses == []: #Nothing matches, so resort to the default...
            for targetClass in classMap["defaultClasses"]:
                targetClasses.append(classUriBase+targetClass)
            for extClass in classMap["defaultExtensions"]:
                targetClasses.append(extUriBase+extClass)
        rootObject = ies.createObject(targetClasses,uri)
    else:
        raise Exception("No classMap specified")
    return rootObject
        


#This function maps the required attributes to IES using the iesLib
def decodeAISLine(line):

    #DSTL Headers:
    #   0       1               2       3       4       5       6           7           8       9           10          11      12      13      14      15
    #  MMSI,    BaseDateTime,   LAT,    LON,    SOG,    COG,    Heading,    VesselName, IMO,    CallSign,   VesselType, Status, Length, Width, Draft,   Cargo

 #OZ format...
       # 0               1           2             3    4       5       6           7       8       9           10
        #CRAFT_ID,      LON,        LAT,        COURSE, SPEED,  TYPE,   SUBTYPE,    LENGTH, BEAM,   DRAUGHT,    TIMESTAMP
        #503444444,     142.99254,  -42.005783, 0,      0,      30,        ,            ,       ,          ,   01/04/2020 12:00:00 AM
        #370123456,     143.5639,   -42.073288, 90,     20,     30,         ,           ,       ,           ,   01/04/2020 12:00:00 AM

    print(line)
    if msgFormat == "OZ":
        mmsi = line[0]
        latitude = line[2]
        longitude = line[1]
        imo = ""
        vesselName = ""
        callsign = ""
        dateTime = line[10]
        course = line[3]
        speed = line[4]
    else:
        mmsi = line[0]
        imo = line[8]
        vesselName = line[7]
        callsign = line[9]
        dateTime = line[1]
        latitude = line[2]
        longitude = line[3]
        course = line[5]
        speed = line[4]     

    #Have we seen this vessel before ?
    if mmsi in cache:
        lastPing = cache[mmsi]
    else:
        lastPing = None
    
    

    #Use the mapping spec to instantiate the vessel and type it
    vesselState = ies.mapRow(line,"ship")
    observation = ies.createObject(ies.iesUri("Observation"),ies.dataURLstub+str(uuid.uuid4()))
    ies.makeTriple(vesselState,ies.iesUri("isParticipantIn"),observation)

    #use imo to identify the ship, as mmsi can change over time
    #IMO identifies the ship through life, 
    if imo != "":
        vesselState.addIdentifier(imo,namingSpaces["imoNS"],ies.iesUri("VehicleIdentificationNumber"),ies.dataURLstub+imo+"IMO")

    #the MMSI identifies the AIS transponder. This can can change as transponders are swapped out. 
    #MMSI can also change (and often does - esp if a ship is renamed or re-flagged) - record mmsi, name, etc. with each location read. Target systems can normalise this on receipt.
    # We create a state for the MMSI, with a URI based on the code. We don't know the start and end dates of the state though.
    vesselState.addIdentifier(mmsi,namingSpaces["mmsiNS"],ies.iesUri("VehicleIdentificationNumber"))

    #as with mmsi, the ship can change name, so again, we create a state:
    if vesselName != "":
        vesselState.addName(vesselName,None,ies.iesUri("VehicleName"))

    #and the same change problem also exists for callsigns, so again we create an undated state and add the 
    if callsign != "":
        vesselState.addIdentifier(callsign,None,ies.iesUri("Callsign"))

    #Now the interesting bit - create a state for the position of the ship, add the date time and location
    vesselState.putInPeriod(dateTime)

    #so this is really nasty. The geo model in IES is very limited in its functionality. inLocation isn't really appropriate for positioning a ship against a lat-long, but it'll have to do until
    #IES is fixed. You certainly wouldn't want to use this for weapons targetting...
    geoPoint = ies.createObject(ies.iesUri("GeoPoint"))
    geoPoint.addIdentifier(latitude,None,ies.iesUri("Latitude"))
    geoPoint.addIdentifier(longitude,None,ies.iesUri("Longitude"))
    vesselState.putInLocation(geoPoint)



#producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
#                         value_serializer=lambda x: 
#                         json.dumps(x).encode('utf-8'))
producer = KafkaProducer(bootstrap_servers=[broker])
consumer = KafkaConsumer(aisTopic)

print("Mapper running - waiting for events dear boy, events")
#Wait for AIS messages coming off the "ais" topic, unzip, decode, load into a clean iesLib graph. 
for msg in consumer:
    print(".",end='',flush=True)
    time1 = time.perf_counter()
    #my_json = zlib.decompress(msg.value).decode('utf8')#.replace("'", '"') 
    my_json = msg.value.decode('utf8')#.replace("'", '"') 
    try:
        print(my_json)
        val = json.loads(str(my_json))
    except:
        print("\033[0;31m Error in JSON:",my_json,"\033[39m")
        quit()
    ies.clearGraph()
    #Having cleared out the graph, we now set some constants for each message - these are IES naming schemes and the organisations that own those naming schemes. 
    namingSpaces = aisIesLib.createNamingSchemes(ies)

    decodeAIS([val])
    NT = ies.graph.serialize(format='nt', indent=4).decode()

    binNT = bytes(NT,"utf-8")
    zipNT = zlib.compress(binNT)
    producer.send(iesTopic, value=binNT)
    time2 = time.perf_counter()
    print("time to process message:"+str(time2-time1))
