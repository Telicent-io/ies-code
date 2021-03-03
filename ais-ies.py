#LICENSE = GPL3 (Gnu Public License v3)
#Produced under contract to Dstl 

from rdflib import Graph, plugin, URIRef, BNode, Literal
from rdflib.namespace import DC, DCAT, DCTERMS, OWL, RDF, RDFS, XMLNS, XSD
from rdflib.serializer import Serializer
import uuid
import Geohash   # (pip install Geohash but also pip install python-geohash )

#Some sample AIS messages. Really simple stuff. As requested, we are not sending course, speed or activity class as that's what they intend to infer from the data
#Format is - mmsi, timestamp (in ISO8601 format), lat, lon
ais=[
    ["366952890","2007-01-01T00:00:09",40.64175,-74.07136],
    ["366952890","2007-01-01T00:01:20",40.64175,-74.07135],
    ["367000150","2007-01-01T00:01:57",40.64196,-74.07289],
    ["366952890","2007-01-01T00:02:29",40.64176,-74.07136],
    ["366952890","2007-01-01T00:03:38",40.64176,-74.07138],
    ["366952890","2007-01-01T00:04:39",40.64176,-74.07138],
    ["367000150","2007-01-01T00:04:55",40.64197,-74.07291],
    ["366952890","2007-01-01T00:05:40",40.64175,-74.07136],
    ["366952890","2007-01-01T00:06:50",40.64175,-74.07135],
    ["367000150","2007-01-01T00:07:56",40.64196,-74.07291]
]

#The URI namespaces we're going to be using
iesUri = "http://ies.data.gov.uk/ontology/ies4#"
ituUri = "http://itu.int"
iso8601Uri = "http://iso.org/iso8601#"
#this data URI can be changed to whatever you want:
dataUri = "http://ais.data.gov.uk/ais-ies-test#"

#Now create some uri objects for the key IES classes we're going to be using - these are just in-memory reference data the python can use to easily instantiate our data
#In production, we automate this from the schema, but that makes it quite hard to follow the code. For this, I've itemised out each class we use so it should make more sense
locationTransponder = URIRef(iesUri+"LocationTransponder")
locationObservation = URIRef(iesUri+"LocationObservation")
observation = URIRef(iesUri+"Observation")
observedLocation = URIRef(iesUri+"ObservedLocation")
observedTarget = URIRef(iesUri+"ObservedTarget")
geoPoint = URIRef(iesUri+"GeoPoint")
particularPeriod = URIRef(iesUri+"ParticularPeriod")
organisation = URIRef(iesUri+"Organisation")
namingScheme = URIRef(iesUri+"NamingScheme")
latitude = URIRef(iesUri+"Latitude")
longitude = URIRef(iesUri+"Longitude")
commsIdentifier = URIRef(iesUri+"CommunicationsIdentifier")
follow = URIRef(iesUri+"Follow")
follower = URIRef(iesUri+"Follower")
followed = URIRef(iesUri+"Followed")
boundingState = URIRef(iesUri+"BoundingState")
possibleWorld = URIRef(iesUri+"PossibleWorld")
system = URIRef(iesUri+"System")
assess = URIRef(iesUri+"Assess")
assessor = URIRef(iesUri+"Assessor")
name = URIRef(iesUri+"Name")

#Now the IES predicates (properties / relationships) we'll be using
ipo = URIRef(iesUri+"isParticipationOf")
ipi = URIRef(iesUri+"isParticipantIn")
iib = URIRef(iesUri+"isIdentifiedBy")
hn = URIRef(iesUri+"hasName")
ip = URIRef(iesUri+"inPeriod")
so = URIRef(iesUri+"schemeOwner")
rv = URIRef(iesUri+"representationValue")
ins = URIRef(iesUri+"inScheme")
ipao = URIRef(iesUri+"isPartOf")
isoP = URIRef(iesUri+"iso8601PeriodRepresentation")
iso = URIRef(iesUri+"isStartOf")
ieo = URIRef(iesUri+"isEndOf")
ass = URIRef(iesUri+"assessed")



mmsiNs = URIRef(ituUri+"#mmsi-NamingScheme") #Make a URI for the MMSI naming schema from the ITU's URI 

#this kinda speaks for itself. Creates a random (UUID) URI based on the dataUri stub
def generateDataUri():
    return(URIRef(dataUri+str(uuid.uuid4())))

#Check to see if a triple is already in our graph
def inGraph(iesGraph,subject,predicate,obj):
    return (subject, predicate, obj) in iesGraph

#We use this function to check if we've already got this triple in the graph before creating it - rdflib should deal with this, but we've had a few issues, and this also helps a bit with performance
def addToGraph(iesGraph,subject,predicate,obj):
    if not inGraph(iesGraph,subject, predicate, obj):
        iesGraph.add((subject, predicate, obj))

#Convenience function to create an instance of a class
def instantiate(iesGraph,_class,instance=None):
    if instance == None:
        #Make a uri based on the data stub...
        instance = generateDataUri()
    addToGraph(iesGraph,instance,RDF.type,_class)
    return(instance)

#Puts an item in a particular period
def putInPeriod(iesGraph,item,iso8601TimeString):
    pp = URIRef(iso8601Uri+str(iso8601TimeString)) #The time is encoded in the URI so we can resolve on unique periods - this code assumes ISO8601 formatted timestamp...standards dear boy, standards !
    instantiate(iesGraph,particularPeriod,pp)
    addToGraph(iesGraph,item,ip,pp)
    return pp

#Asserts an item started in a particular period
def startsIn(iesGraph,item,iso8601TimeString):
    bs = instantiate(iesGraph,boundingState)
    addToGraph(iesGraph,bs,iso,item)
    putInPeriod(iesGraph,bs,iso8601TimeString)

#Asserts an item ended in a particular period
def endsIn(iesGraph,item,iso8601TimeString):
    bs = instantiate(iesGraph,boundingState)
    addToGraph(iesGraph,bs,ieo,item)
    putInPeriod(iesGraph,bs,iso8601TimeString)

def addName(iesGraph,item,nameString,nameType=None,namingScheme=None):
    if not nameType:
        nameType = name
    myName = instantiate(iesGraph,nameType)
    addToGraph(iesGraph,myName,rv,Literal(nameString, datatype=XSD.string))
    addToGraph(iesGraph,item,hn,myName)
    if namingScheme:
        addToGraph(iesGraph,myName,ins,namingScheme)
    return myName

#add boilerplate stuff
def addNamingSchemes(iesGraph):
    #Boiler plate stuff - creating the NamingScheme for mmsi:
    addToGraph(iesGraph,mmsiNs, RDF.type, namingScheme)#Note that RDF lib prefers to be given URIRefs...it will also work with strings but we hit some snags and now always force a URIRef for subject predicate and object
    #congrats, you just created your first IES triple. Woop Woop.
    #Now let's make the ITU the owner of the namingScheme
    instantiate(iesGraph, organisation,URIRef(ituUri))
    #Now add a name for the ITU organisation
    addName(iesGraph,URIRef(ituUri),"International Telecommunications Union")
    #Make the ITU the owner of that naming scheme
    addToGraph(iesGraph,mmsiNs,so,URIRef(ituUri))

def createLocationTransponder(iesGraph,mmsi):
    #COnstruct a URI based on its mmsi
    myLT = URIRef(dataUri+"MMSI_"+mmsi)
    #Check to see if we already have it in the graph, and create if not - rdflib should just overwrite, but we've seen some exceptions so best to check
    if not inGraph(iesGraph,myLT,RDF.type,locationTransponder):
        instantiate(iesGraph,locationTransponder,myLT)
        #Add the id object
        ltId = URIRef(dataUri+"MMSI_"+mmsi+"_idObj")
        instantiate(iesGraph,commsIdentifier,ltId)
        addToGraph(iesGraph,ltId,rv,Literal(mmsi, datatype=XSD.string))
        addToGraph(iesGraph,myLT,iib,ltId)
        #Now put the comms ID in the naming scheme...
        addToGraph(iesGraph,ltId,ins,mmsiNs)
    return myLT

#Simple function to process a line of AIS in IES. It expects mmsi, timestamp (in ISO8601 format), lat, lon
def createLocationObservation(iesGraph,mmsi,timestamp,lat,lon,obs=None):
    print(mmsi,timestamp)
    #add the location transponder - We don't know this is necessarily a vessel. All we know is that we have a LocationTransponder. 
    lt = createLocationTransponder(iesGraph,mmsi)
    #Now create the observation event
    lo = instantiate(iesGraph,locationObservation)
    #If track emulation is not required, obs will be None. If it's not None, make the LocationObservation (lo) part of the overall track observation
    if obs:
        addToGraph(iesGraph,lo,ipao,obs)
    #...and the ParticularPeriod in which the observation occurred
    putInPeriod(iesGraph,lo,timestamp)
    #And involve the transponder in that location observation
    ltPart = instantiate(iesGraph,observedTarget)
    addToGraph(iesGraph,ltPart,ipo,lt) #participation of the transponder
    addToGraph(iesGraph,ltPart,ipi,lo) #participation in the LocationObservation
    #Now the observed location, a geopoint with a lat and long - using a geohash to give each point a unique uri
    gp = URIRef(dataUri+"geohash_"+Geohash.encode(lat,lon))
    instantiate(iesGraph,geoPoint,gp)
    #Add the lat and long values as identifiers of the geopoint...firstly creating repeatable URIs for them so they don't overwrite
    latObj = URIRef(gp.toPython()+"_lat")
    lonObj = URIRef(gp.toPython()+"_lon")
    instantiate(iesGraph, latitude,latObj)
    instantiate(iesGraph, longitude,lonObj)
    addToGraph(iesGraph,gp,iib,latObj)
    addToGraph(iesGraph,gp,iib,lonObj)
    #Add the representation values to the lat and lon objects
    addToGraph(iesGraph,latObj,rv,Literal(lat, datatype=XSD.string))
    addToGraph(iesGraph,lonObj,rv,Literal(lon, datatype=XSD.string))
    #Now the participation of the GeoPoint in the Observation
    gpPart = instantiate(iesGraph,observedLocation)
    addToGraph(iesGraph,gpPart,ipo,gp) #participation of the GeoPoint
    addToGraph(iesGraph,gpPart,ipi,lo) #participation in the LocationObservation


#A simple parent observation to group the others into track
def createTrack(iesGraph):
    return instantiate(iesGraph,observation)



def saveRdf(graph,filename):
    graph.serialize(destination=filename, format='ttl')
    graph.remove((None, None, None)) #Clear the graph 

def createSystem(iesGraph,sysName):
    sys = instantiate(iesGraph,system)

def following(iesGraph,followerMMSI, followedMMSI, startTimeStamp, endTimeStamp,inferenceSystem):
    #Create the follow event object
    fol = instantiate(iesGraph,follow)
    #Now put the start and end on the following event...
    startsIn(iesGraph,fol,startTimeStamp)
    endsIn(iesGraph,fol,endTimeStamp)

    #create this follower and followed transponders - these may already be i the graph, but this should get caught
    followerURI = createLocationTransponder(iesGraph,followerMMSI)
    followedURI = createLocationTransponder(iesGraph,followedMMSI)
    #Now create the participations of the follower and followed
    folrPart = instantiate(iesGraph,follower)
    addToGraph(iesGraph,folrPart,ipi,fol)
    addToGraph(iesGraph,folrPart,ipo,followerURI)
    foldPart = instantiate(iesGraph,followed)
    addToGraph(iesGraph,foldPart,ipi,fol)
    addToGraph(iesGraph,foldPart,ipo,followedURI)
    if (inferenceSystem):
        pw = instantiate(iesGraph,possibleWorld)
        addToGraph(iesGraph,fol,ipao,pw)
        assessment = instantiate(iesGraph,assess)
        addToGraph(iesGraph,assessment,ass,pw)
        assr = instantiate(iesGraph,assessor)
        addToGraph(iesGraph,assr,ipi,assessment)
        addToGraph(iesGraph,assr,ipo,inferenceSystem)

    

#set up the RDF graph
graph = Graph()
graph.namespace_manager.bind('ies', iesUri)
graph.namespace_manager.bind('iso8601', iso8601Uri)
graph.namespace_manager.bind('data', dataUri)
addNamingSchemes(graph)
#Add a parent observation
#obs = createTrack(graph) #comment this line out to prevent a track being created
#run the positions data through
#for aisLine in ais:
 #   createLocationObservation(aisLine[0],aisLine[1],aisLine[2],aisLine[3],obs,graph)

#Now say one is following the other (they're not, but I didn't have any data where ships followed each other)
#First we create an object for the system that detected it. 
hal = instantiate(graph,system)
addName(graph,hal,"HAL")
#now create the following pattern, with our system included as the assessor
following(graph,"367000150","366952890","2007-01-01T00:00:09","2007-01-01T00:05:40",hal)

saveRdf(graph,'output.ies.ttl')