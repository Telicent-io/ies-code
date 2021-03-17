#LICENSE = GPL3 (Gnu Public License v3)
#Produced under contract to Dstl 

from rdflib import Graph, plugin, URIRef, BNode, Literal
from rdflib.namespace import DC, DCAT, DCTERMS, OWL, RDF, RDFS, XMLNS, XSD
from rdflib.serializer import Serializer
import uuid
import dateutil.parser #required to do earliest, latest date time comparison
import Geohash   # (pip install Geohash but also pip install python-geohash )

#Some sample AIS messages. Really simple stuff. As requested, we are not sending course, speed or activity class as that's what they intend to infer from the data
#Format is - mmsi, timestamp (in ISO8601 format), lat, lon
ais=[
    ["366952890","2007-01-01T00:00:09",40.64175,-74.07136],
    ["366952890","2007-01-01T00:01:20",40.64175,-74.07135],
    ["366952890","2007-01-01T00:02:29",40.64176,-74.07136],
    ["366952890","2007-01-01T00:03:38",40.64176,-74.07138],
    ["366952890","2007-01-01T00:04:39",40.64176,-74.07138],
    ["366952890","2007-01-01T00:05:40",40.64175,-74.07136],
    ["366952890","2007-01-01T00:06:50",40.64175,-74.07135],
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

#delete all triples in the graph
def clearGraph(iesGraph):
    iesGraph.remove((None, None, None))

#clears the graph and adds all the boilerplate stuff
def initialiseGraph(iesGraph):
    clearGraph(iesGraph=iesGraph)
    iesGraph.namespace_manager.bind('ies', iesUri)
    iesGraph.namespace_manager.bind('iso8601', iso8601Uri)
    iesGraph.namespace_manager.bind('data', dataUri)
    addNamingSchemes(iesGraph=iesGraph)
    return iesGraph


#this kinda speaks for itself. Creates a random (UUID) URI based on the dataUri stub
def generateDataUri():
    return(URIRef(dataUri+str(uuid.uuid4())))

#Check to see if a triple is already in our graph
def inGraph(iesGraph,subject,predicate,obj):
    return (subject, predicate, obj) in iesGraph

#We use this function to check if we've already got this triple in the graph before creating it - rdflib should deal with this, but we've had a few issues, and this also helps a bit with performance
def addToGraph(iesGraph,subject,predicate,obj):
    if not inGraph(iesGraph=iesGraph,subject=subject, predicate=predicate, obj=obj):
        iesGraph.add((subject, predicate, obj))

#Convenience function to create an instance of a class
def instantiate(iesGraph,_class,instance=None):
    if instance == None:
        #Make a uri based on the data stub...
        instance = generateDataUri()
    addToGraph(iesGraph=iesGraph,subject=instance,predicate=RDF.type,obj=_class)
    return(instance)

#Puts an item in a particular period
def putInPeriod(iesGraph,item,iso8601TimeString):
    pp = URIRef(iso8601Uri+str(iso8601TimeString)) #The time is encoded in the URI so we can resolve on unique periods - this code assumes ISO8601 formatted timestamp...standards dear boy, standards !
    instantiate(iesGraph=iesGraph,_class=particularPeriod,instance=pp)
    addToGraph(iesGraph=iesGraph,subject=item,predicate=ip,obj=pp)
    return pp

#Asserts an item started in a particular period
def startsIn(iesGraph,item,iso8601TimeString):
    bs = instantiate(iesGraph=iesGraph,_class=boundingState)
    addToGraph(iesGraph=iesGraph,subject=bs,predicate=iso,obj=item)
    putInPeriod(iesGraph=iesGraph,item=bs,iso8601TimeString=iso8601TimeString)

#Asserts an item ended in a particular period
def endsIn(iesGraph,item,iso8601TimeString):
    bs = instantiate(iesGraph=iesGraph,_class=boundingState)
    addToGraph(iesGraph=iesGraph,subject=bs,predicate=ieo,obj=item)
    putInPeriod(iesGraph=iesGraph,item=bs,iso8601TimeString=iso8601TimeString)

def addName(iesGraph,item,nameString,nameType=None,namingScheme=None):
    if not nameType:
        nameType = name
    myName = instantiate(iesGraph,nameType)
    addToGraph(iesGraph=iesGraph,subject=myName,predicate=rv,obj=Literal(nameString, datatype=XSD.string))
    addToGraph(iesGraph=iesGraph,subject=item,predicate=hn,obj=myName)
    if namingScheme:
        addToGraph(iesGraph=iesGraph,subject=myName,predicate=ins,obj=namingScheme)
    return myName

#add boilerplate stuff
def addNamingSchemes(iesGraph):
    #Boiler plate stuff - creating the NamingScheme for mmsi:
    addToGraph(iesGraph=iesGraph,subject=mmsiNs, predicate=RDF.type, obj=namingScheme)#Note that RDF lib prefers to be given URIRefs...it will also work with strings but we hit some snags and now always force a URIRef for subject predicate and object
    #congrats, you just created your first IES triple. Woop Woop.
    #Now let's make the ITU the owner of the namingScheme
    instantiate(iesGraph=iesGraph, _class=organisation,instance=URIRef(ituUri))
    #Now add a name for the ITU organisation
    addName(iesGraph=iesGraph,item=URIRef(ituUri),nameString="International Telecommunications Union")
    #Make the ITU the owner of that naming scheme
    addToGraph(iesGraph=iesGraph,subject=mmsiNs,predicate=so,obj=URIRef(ituUri))

def createLocationTransponder(iesGraph,mmsi):
    #COnstruct a URI based on its mmsi
    myLT = URIRef(dataUri+"MMSI_"+mmsi)
    #Check to see if we already have it in the graph, and create if not - rdflib should just overwrite, but we've seen some exceptions so best to check
    if not inGraph(iesGraph=iesGraph,subject=myLT,predicate=RDF.type,obj=locationTransponder):
        instantiate(iesGraph=iesGraph,_class=locationTransponder,instance=myLT)
        #Add the id object
        ltId = URIRef(dataUri+"MMSI_"+mmsi+"_idObj")
        instantiate(iesGraph=iesGraph,_class=commsIdentifier,instance=ltId)
        addToGraph(iesGraph=iesGraph,subject=ltId,predicate=rv,obj=Literal(mmsi, datatype=XSD.string))
        addToGraph(iesGraph=iesGraph,subject=myLT,predicate=iib,obj=ltId)
        #Now put the comms ID in the naming scheme...
        addToGraph(iesGraph=iesGraph,subject=ltId,predicate=ins,obj=mmsiNs)
    return myLT

#Simple function to process a line of AIS in IES. It expects mmsi, timestamp (in ISO8601 format), lat, lon
def createLocationObservation(iesGraph,mmsi,timestamp,lat,lon,obs=None):
    print(mmsi,timestamp)
    #add the location transponder - We don't know this is necessarily a vessel. All we know is that we have a LocationTransponder. 
    lt = createLocationTransponder(iesGraph=iesGraph,mmsi=mmsi)
    #Now create the observation event
    lo = instantiate(iesGraph=iesGraph,_class=locationObservation)
    #If track emulation is not required, obs will be None. If it's not None, make the LocationObservation (lo) part of the overall track observation
    if obs:
        addToGraph(iesGraph=iesGraph,subject=lo,predicate=ipao,obj=obs)
    #...and the ParticularPeriod in which the observation occurred
    putInPeriod(iesGraph=iesGraph,item=lo,iso8601TimeString=timestamp)
    #And involve the transponder in that location observation
    ltPart = instantiate(iesGraph=iesGraph,_class=observedTarget)
    addToGraph(iesGraph=iesGraph,subject=ltPart,predicate=ipo,obj=lt) #participation of the transponder
    addToGraph(iesGraph=iesGraph,subject=ltPart,predicate=ipi,obj=lo) #participation in the LocationObservation
    #Now the observed location, a geopoint with a lat and long - using a geohash to give each point a unique uri
    gp = URIRef(dataUri+"geohash_"+Geohash.encode(lat,lon))
    instantiate(iesGraph=iesGraph,_class=geoPoint,instance=gp)
    #Add the lat and long values as identifiers of the geopoint...firstly creating repeatable URIs for them so they don't overwrite
    latObj = URIRef(gp.toPython()+"_lat")
    lonObj = URIRef(gp.toPython()+"_lon")
    instantiate(iesGraph=iesGraph, _class=latitude,instance=latObj)
    instantiate(iesGraph=iesGraph, _class=longitude,instance=lonObj)
    addToGraph(iesGraph=iesGraph,subject=gp,predicate=iib,obj=latObj)
    addToGraph(iesGraph=iesGraph,subject=gp,predicate=iib,obj=lonObj)
    #Add the representation values to the lat and lon objects
    addToGraph(iesGraph=iesGraph,subject=latObj,predicate=rv,obj=Literal(lat, datatype=XSD.string))
    addToGraph(iesGraph=iesGraph,subject=lonObj,predicate=rv,obj=Literal(lon, datatype=XSD.string))
    #Now the participation of the GeoPoint in the Observation
    gpPart = instantiate(iesGraph=iesGraph,_class=observedLocation)
    addToGraph(iesGraph=iesGraph,subject=gpPart,predicate=ipo,obj=gp) #participation of the GeoPoint
    addToGraph(iesGraph=iesGraph,subject=gpPart,predicate=ipi,obj=lo) #participation in the LocationObservation


#A simple parent observation to group the others into track
def createTrack(iesGraph):
    return instantiate(iesGraph=iesGraph,_class=observation)



def saveRdf(graph,filename):
    graph.serialize(destination=filename, format='ttl')
    graph.remove((None, None, None)) #Clear the graph 

def createSystem(iesGraph,sysName):
    sys = instantiate(iesGraph=iesGraph,_class=system)

def following(iesGraph,followerMMSI, followedMMSI, startTimeStamp, endTimeStamp,inferenceSystem):
    #Create the follow event object
    fol = instantiate(iesGraph=iesGraph,_class=follow)
    #Now put the start and end on the following event...
    startsIn(iesGraph=iesGraph,item=fol,iso8601TimeString=startTimeStamp)
    endsIn(iesGraph=iesGraph,item=fol,iso8601TimeString=endTimeStamp)

    #create this follower and followed transponders - these may already be i the graph, but this should get caught
    followerURI = createLocationTransponder(iesGraph=iesGraph,mmsi=followerMMSI)
    followedURI = createLocationTransponder(iesGraph=iesGraph,mmsi=followedMMSI)
    #Now create the participations of the follower and followed
    folrPart = instantiate(iesGraph=iesGraph,_class=follower)
    addToGraph(iesGraph=iesGraph,subject=folrPart,predicate=ipi,obj=fol)
    addToGraph(iesGraph=iesGraph,subject=folrPart,predicate=ipo,obj=followerURI)
    foldPart = instantiate(iesGraph=iesGraph,_class=followed)
    addToGraph(iesGraph=iesGraph,subject=foldPart,predicate=ipi,obj=fol)
    addToGraph(iesGraph=iesGraph,subject=foldPart,predicate=ipo,obj=followedURI)
    if (inferenceSystem):
        pw = instantiate(iesGraph=iesGraph,_class=possibleWorld)
        addToGraph(iesGraph=iesGraph,subject=fol,predicate=ipao,obj=pw)
        assessment = instantiate(iesGraph=iesGraph,_class=assess)
        addToGraph(iesGraph=iesGraph,subject=assessment,predicate=ass,obj=pw)
        assr = instantiate(iesGraph=iesGraph,_class=assessor)
        addToGraph(iesGraph=iesGraph,subject=assr,predicate=ipi,obj=assessment)
        addToGraph(iesGraph=iesGraph,subject=assr,predicate=ipo,obj=inferenceSystem)
        #Now put the assessment in a period - i.e. when the assessment was done
        putInPeriod(iesGraph=iesGraph,item=assessment,iso8601TimeString="2007-01-02T09:17:04")



#set up the RDF graph
graph = Graph()
initialiseGraph(graph)
#Add a parent observation
obs = createTrack(graph) #comment this line out to prevent a track being created
#run the positions data through
earliestTimeStamp = None
latestTimeStamp = None
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
startsIn(iesGraph=graph,item=obs,iso8601TimeString=earliestTimeStamp.isoformat())
endsIn(iesGraph=graph,item=obs,iso8601TimeString=latestTimeStamp.isoformat())

saveRdf(graph,'track.ies.ttl')

#erase and re-initialise the graph - comment this out if you want the additional data to go into the same graph.
initialiseGraph(graph)

#Now say one is following the other (they're not, but I didn't have any data where ships followed each other)
#First we create an object for the system that detected it. 
hal = instantiate(iesGraph=graph,_class=system)
addName(iesGraph=graph,item=hal,nameString="HAL")
#now create the following pattern, with our system included as the assessor
following(iesGraph=graph,followerMMSI="367000150",followedMMSI="366952890",startTimeStamp="2007-01-01T00:00:09",endTimeStamp="2007-01-01T00:05:40",inferenceSystem=hal)

saveRdf(graph,'following.ies.ttl')