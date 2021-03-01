#LICENSE = GPL3 (Gnu Public License v3)

from rdflib import Graph, plugin, URIRef, BNode, Literal
from rdflib.namespace import DC, DCAT, DCTERMS, OWL, RDF, RDFS, XMLNS, XSD
from rdflib.serializer import Serializer
import uuid
import Geohash   # (pip install Geohash but also pip install python-geohash )

#Some sample AIS messages. Really simple stuff. At the request of CAN, we are not sending course, speed or activity class as that's what they intend to infer from the data
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


#Now the IES predicates (properties / relationships) we'll be using
ipo = URIRef(iesUri+"isParticipationOf")
ipi = URIRef(iesUri+"isParticipantIn")
iib = URIRef(iesUri+"isIdentifiedBy")
hn = URIRef(iesUri+"hasName")
ip = URIRef(iesUri+"inPeriod")
so = URIRef(iesUri+"schemeOwner")
rv = URIRef(iesUri+"representationValue")
ins = URIRef(iesUri+"inScheme")
ipa = URIRef(iesUri+"isPartOf")

mmsiNs = URIRef(ituUri+"#mmsi-NamingScheme") #Make a URI for the MMSI naming schema from the ITU's URI 

#Global variable (we love those) for the rdflib graph object. This is the lib we will use to format the IES data
graph = Graph()
graph.namespace_manager.bind('ies', iesUri)
graph.namespace_manager.bind('iso8601', iso8601Uri)
graph.namespace_manager.bind('data', dataUri)

#this kinda speaks for itself. Creates a random (UUID) URI based on the dataUri stub
def generateDataUri():
    return(URIRef(dataUri+str(uuid.uuid4())))

#We use this function to check if we've already got this triple in the graph before creating it - rdflib should deal with this, but we've had a few issues, and this also helps a bit with performance
def addToGraph(graph,subject,predicate,obj):
    print(subject, predicate, obj)
    if (subject, predicate, obj) not in graph:
        graph.add((subject, predicate, obj))

#Convenience function to create an instance of a class
def instantiate(graph,_class,instance=None):
    if instance == None:
        #Make a uri based on the data stub...
        instance = generateDataUri()
    addToGraph(graph,instance,RDF.type,_class)
    return(instance)

#add boilerplate stuff
def addNamingSchemes(iesGraph):
    #Boiler plate stuff - creating the NamingScheme for mmsi:
    addToGraph(iesGraph,mmsiNs, RDF.type, namingScheme)#Note that RDF lib prefers to be given URIRefs...it will also work with strings but we hit some snags and now always force a URIRef for subject predicate and object
    #congrats, you just created your first IES triple. Woop Woop.
    #Now let's make the ITU the owner of the namingScheme
    instantiate(iesGraph, organisation,URIRef(ituUri))
    #Now add a name for the ITU organisation
    ituName = Literal("International Telecommunications Union", datatype=XSD.string) #wrap the text in a literal object to keep rdflib happy !
    addToGraph(iesGraph,URIRef(ituUri),hn,ituName)
    #Make the ITU the owner of that naming scheme
    addToGraph(iesGraph,mmsiNs,so,URIRef(ituUri))

#Simple function to process a line of AIS in IES. It expects mmsi, timestamp (in ISO8601 format), lat, lon
def mapAisPing(mmsi,timestamp,lat,lon,obs,iesGraph):
    #add the location transponder - We don't know this is necessarily a vessel. All we know is that we have a LocationTransponder. Here it is:
    lt = URIRef(dataUri+"MMSI_"+mmsi)
    instantiate(iesGraph,locationTransponder,lt) #This may already be in our dataset, byt addToGraph() function will spot this and it won't get written twice. RDFLib usually catches these, but belt'n'braces
    #Add the id object
    ltId = URIRef(dataUri+"MMSI_"+mmsi+"_idObj")
    instantiate(iesGraph,commsIdentifier,ltId)
    addToGraph(iesGraph,ltId,rv,Literal(mmsi, datatype=XSD.string))
    addToGraph(iesGraph,lt,iib,ltId)
    #Now put the comms ID in teh naming scheme...
    addToGraph(iesGraph,ltId,ins,mmsiNs)
    #Now create the observation event
    lo = instantiate(iesGraph,locationObservation)
    addToGraph(iesGraph,lo,ipa,obs)
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
    addToGraph(iesGraph,latObj,rv,Literal(lat, datatype=XSD.decimal))
    addToGraph(iesGraph,lonObj,rv,Literal(lon, datatype=XSD.decimal))
    #Now the participation of the GeoPoint in the Observation
    gpPart = instantiate(iesGraph,observedLocation)
    addToGraph(iesGraph,gpPart,ipo,gp) #participation of the GeoPoint
    addToGraph(iesGraph,gpPart,ipi,lo) #participation in the LocationObservation
   # ns["itu"] = ies.createObject([ies.iesUri("Organisation")],ituURI())
  #  ns["itu"].addName("International Telecommunications Union",None,ies.iesUri("OrganisationName"))
   # ns["imo"] = ies.createObject([ies.iesUri("Organisation")],imoURI())
  #  ns["imo"].addName("International Maritime Organisation",None,ies.iesUri("OrganisationName"))
  #  ns["mmsiNS"] = ies.createNamingScheme(mmsiNSURI(),ns["itu"])
  #  ns["imoNS"] = ies.createNamingScheme(imoNSURI(),ns["imo"])

    #

#Run through the demo data and spit out the IES to stdout
def testAIS():
    addNamingSchemes(graph)
    #To simulate this being a track, create a parent observation that all the location observations are part of:
    obs = instantiate(graph,observation)
    for aisLine in ais:
        mapAisPing(aisLine[0],aisLine[1],aisLine[2],aisLine[3],obs,graph)
        rdfOut = graph.serialize(format="turtle").decode("utf-8") #Turtle(ttl) is a fairly readable format for RDF data
    print(rdfOut)
    graph.serialize(destination='output.ies.ttl', format='ttl')

testAIS()