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
identifier = URIRef(iesUri+"Identifier")
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
epsgParams = [URIRef(iesUri+"EpsgParameter1"),URIRef(iesUri+"EpsgParameter2"),URIRef(iesUri+"EpsgParameter3"),URIRef(iesUri+"EpsgParameter4")]
epsgRep = URIRef(iesUri+"EpsgGeoPointRepresentation")
measure = URIRef(iesUri+"Measure")
measurement = URIRef(iesUri+"Measurement")
measureValue = URIRef(iesUri+"MeasureValue")
unitOfMeasure = URIRef(iesUri+"UnitOfMeasure")



#Now the IES predicates (properties / relationships) we'll be using
ass = URIRef(iesUri+"assessed")
hn = URIRef(iesUri+"hasName")
hv = URIRef(iesUri+"hasValue")
ieo = URIRef(iesUri+"isEndOf")
iib = URIRef(iesUri+"isIdentifiedBy")
ins = URIRef(iesUri+"inScheme")
ip = URIRef(iesUri+"inPeriod")
ipao = URIRef(iesUri+"isPartOf")
ipi = URIRef(iesUri+"isParticipantIn")
ipo = URIRef(iesUri+"isParticipationOf")
ir = URIRef(iesUri+"inRepresentation")
iso = URIRef(iesUri+"isStartOf")
isoP = URIRef(iesUri+"iso8601PeriodRepresentation")
mu = URIRef(iesUri+"measureUnit")
rv = URIRef(iesUri+"representationValue")
so = URIRef(iesUri+"schemeOwner")
#New stuff, not yet approved !
ec = URIRef(iesUri+"epsgCode")
mc = URIRef(iesUri+"measureClass")
och = URIRef(iesUri+"observedCharacteristic")


mmsiNs = URIRef(ituUri+"#mmsi-NamingScheme") #Make a URI for the MMSI naming schema from the ITU's URI 


def setDataUri(uri):
    global dataUri
    dataUri = uri

#delete all triples in the graph
def clearGraph(iesGraph):
    iesGraph.remove((None, None, None))

#clears the graph and adds all the boilerplate stuff
def initialiseGraph(iesGraph=None):
    if iesGraph is None:
        iesGraph = Graph()
    clearGraph(iesGraph=iesGraph)
    iesGraph.namespace_manager.bind('ies', iesUri)
    iesGraph.namespace_manager.bind('iso8601', iso8601Uri)
    iesGraph.namespace_manager.bind('data', dataUri)
    addNamingSchemes(iesGraph=iesGraph)
    return iesGraph

def getShortUri(graph,uri):
    if type(uri) is URIRef:
        id = uri
    else:
        id = URIRef(uri)
    return id.n3(graph.namespace_manager)

def parseJSONLD(iesGraph, rawJSON):
    # assume it's compressed JSON-LD, so flatten it !
    flattened = jsonld.flatten(rawJSON)
    iesGraph.parse(data=json.dumps(flattened), format='json-ld')

def parseN3(iesGraph, n3):
    # assume it's compressed JSON-LD, so flatten it !
    iesGraph.parse(data=n3, format='n3')

def parseNT(iesGraph, nt):
    # assume it's compressed JSON-LD, so flatten it !
    iesGraph.parse(data=nt, format='nt')

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
    addToGraph(iesGraph=iesGraph,subject=pp,predicate=isoP,obj=Literal(str(iso8601TimeString), datatype=XSD.string))
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

#addMeasure - creates a measure that's an instance of a given measureClass, adds its value and unit of measure
def addMeasure(iesGraph,measureClass,value,uom):
    meas = instantiate(iesGraph=iesGraph,_class=measure)
    addToGraph(iesGraph=iesGraph,subject=meas,predicate=mc,obj=measureClass)
    measureVal = instantiate(iesGraph=iesGraph,_class=measureValue)
    addToGraph(iesGraph=iesGraph,subject=measureVal,predicate=rv,obj=Literal(value, datatype=XSD.string))
    addToGraph(iesGraph=iesGraph,subject=meas,predicate=hv,obj=measureVal)
    addToGraph(iesGraph=iesGraph,subject=measureVal,predicate=mu,obj=uom)
    return(meas)



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

def addIdentifier(iesGraph,identifiedItem,idText,idUri=URIRef(generateDataUri()),idClass=identifier,idRelType = iib,namingScheme=None):
    instantiate(iesGraph=iesGraph,_class=identifier,instance=idUri)
    addToGraph(iesGraph=iesGraph,subject=idUri,predicate=rv,obj=Literal(idText, datatype=XSD.string))
    addToGraph(iesGraph=iesGraph,subject=identifiedItem,predicate=idRelType,obj=idUri)
    if namingScheme:
        addToGraph(iesGraph=iesGraph,subject=idUri,predicate=ins,obj=namingScheme)

def createIdentifiedEntity(iesGraph,entityClass,entityID,idClass=identifier,namingScheme=None,uri=None):
    if idClass==mmsiNs:
        uri = URIRef(dataUri+"MMSI_"+entityID)
        idObj = URIRef(dataUri+"MMSI_"+entityID+"_idObj")
        addIdentifier(iesGraph,uri,entityID,idUri=idObj,idClass=commsIdentifier,namingScheme=mmsiNs)
    else:
        if uri == None:
            uri = generateDataUri()
        addIdentifier(iesGraph,uri,entityID,idClass=idClass,namingScheme=namingScheme)

    obsEnt = instantiate(iesGraph,entityClass,uri)
    return(uri)

#This is used in both the following and track use cases.
def createLocationTransponder(iesGraph,mmsi):
    return createIdentifiedEntity(iesGraph=iesGraph,entityClass=locationTransponder,entityID=mmsi,namingScheme=mmsiNs)


#Instantiate an IES System class
def createSystem(iesGraph,sysName):
    sys = instantiate(iesGraph=iesGraph,_class=system)


def saveRdf(graph,filename):
    graph.serialize(destination=filename, format='ttl')
    graph.remove((None, None, None)) #Clear the graph 


def initialiseKafka(kHost):
    return KafkaProducer(bootstrap_servers=[kHost])

def sendToKafka(iesGraph,kProducer,kTopic):
    #NT = iesGraph.serialize(format='nt', indent=4).decode()
    #binNT = bytes(NT,"utf-8")
    #zipNT = zlib.compress(binNT)
    #kProducer.send(kTopic, value=zipNT)
    ttl = iesGraph.serialize(format='ttl', indent=4).decode()
    binTTL = bytes(ttl,"utf-8")
    kProducer.send(kTopic, value=binTTL)