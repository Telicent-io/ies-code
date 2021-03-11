from rdflib import Graph, plugin, URIRef, BNode, Literal
from rdflib.namespace import DC, DCAT, DCTERMS, OWL, RDF, RDFS, XMLNS, XSD
from rdflib.serializer import Serializer
import uuid
import dateutil.parser
from kafka import KafkaConsumer, KafkaProducer
import zlib



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
#New stuff, not yet approved !
classOfMeasure = URIRef(iesUri+"ClassOfMeasure")
measurement = URIRef(iesUri+"Measurement")
unitOfMeasure = URIRef(iesUri+"UnitOfMeasure")
measure = URIRef(iesUri+"Measure")
measureValue = URIRef(iesUri+"MeasureValue")

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
#New stuff, not yet approved !
och = URIRef(iesUri+"observedCharacteristic")
mu = URIRef(iesUri+"measureUnit")

mmsiNs = URIRef(ituUri+"#mmsi-NamingScheme") #Make a URI for the MMSI naming schema from the ITU's URI 

#delete all triples in the graph
def clearGraph(iesGraph):
    iesGraph.remove((None, None, None))

#clears the graph and adds all the boilerplate stuff
def initialiseGraph(iesGraph):
    if iesGraph is None:
        iesGraph = Graph()
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
def putInPeriod(iesGraph,item,timeString):
    iso8601TimeString = dateutil.parser.parse(timeString).isoformat()
    pp = URIRef(iso8601Uri+str(iso8601TimeString)) #The time is encoded in the URI so we can resolve on unique periods - this code assumes ISO8601 formatted timestamp...standards dear boy, standards !
    instantiate(iesGraph=iesGraph,_class=particularPeriod,instance=pp)
    addToGraph(iesGraph=iesGraph,subject=item,predicate=ip,obj=pp)
    return pp

#Asserts an item started in a particular period
def startsIn(iesGraph,item,timeString):
    bs = instantiate(iesGraph=iesGraph,_class=boundingState)
    addToGraph(iesGraph=iesGraph,subject=bs,predicate=iso,obj=item)
    putInPeriod(iesGraph=iesGraph,item=bs,timeString=timeString)

#Asserts an item ended in a particular period
def endsIn(iesGraph,item,timeString):
    bs = instantiate(iesGraph=iesGraph,_class=boundingState)
    addToGraph(iesGraph=iesGraph,subject=bs,predicate=ieo,obj=item)
    putInPeriod(iesGraph=iesGraph,item=bs,timeString=timeString)

#Add a name to an itme, and optionally specify a particular type of name( from IES Model) and a naming scheme (of your own creation)
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
    addName(iesGraph=iesGraph,item=mmsiNs,nameString="MMSI",namingScheme=mmsiNs) #Hoisted by its own petard...or at least named by its own naming scheme
    #congrats, you just created your first IES triple. Woop Woop.
    #Now let's make the ITU the owner of the namingScheme
    instantiate(iesGraph=iesGraph, _class=organisation,instance=URIRef(ituUri))
    #Now add a name for the ITU organisation
    addName(iesGraph=iesGraph,item=URIRef(ituUri),nameString="International Telecommunications Union")
    #Make the ITU the owner of that naming scheme
    addToGraph(iesGraph=iesGraph,subject=mmsiNs,predicate=so,obj=URIRef(ituUri))

#This is used in both the following and track use cases.
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

#Instantiate an IES System class
def createSystem(iesGraph,sysName):
    sys = instantiate(iesGraph=iesGraph,_class=system)


def saveRdf(graph,filename):
    graph.serialize(destination=filename, format='ttl')
    graph.remove((None, None, None)) #Clear the graph 


def initialiseKafka(kHost):
    return KafkaProducer(bootstrap_servers=[kHost])

def sendToKafka(iesGraph,kProducer,kTopic):
    NT = iesGraph.serialize(format='nt', indent=4).decode()
    binNT = bytes(NT,"utf-8")
    zipNT = zlib.compress(binNT)
    kProducer.send(kTopic, value=zipNT)