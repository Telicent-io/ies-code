from rdflib import Graph, plugin, URIRef, BNode, Literal
from rdflib.namespace import DC, DCAT, DCTERMS, OWL, RDF, RDFS, XMLNS, XSD
import ies_functions

def following(iesGraph,followerMMSI, followedMMSI, startTimeStamp, endTimeStamp,inferenceSystem):
    #Create the follow event object
    fol = ies.instantiate(iesGraph=iesGraph,_class=ies.follow)
    #Now put the start and end on the following event...
    ies.startsIn(iesGraph=iesGraph,item=fol,iso8601TimeString=startTimeStamp)
    ies.endsIn(iesGraph=iesGraph,item=fol,iso8601TimeString=endTimeStamp)

    #create this follower and followed transponders - these may already be i the graph, but this should get caught
    followerURI = ies.createLocationTransponder(iesGraph=iesGraph,mmsi=followerMMSI)
    followedURI = ies.createLocationTransponder(iesGraph=iesGraph,mmsi=followedMMSI)
    #Now create the participations of the follower and followed
    folrPart = ies.instantiate(iesGraph=iesGraph,_class=ies.follower)
    ies.addToGraph(iesGraph=iesGraph,subject=folrPart,predicate=ies.vipi,obj=fol)
    ies.addToGraph(iesGraph=iesGraph,subject=folrPart,predicate=ies.ipo,obj=followerURI)
    foldPart = ies.instantiate(iesGraph=iesGraph,_class=followed)
    ies.addToGraph(iesGraph=iesGraph,subject=foldPart,predicate=ies.ipi,obj=fol)
    ies.addToGraph(iesGraph=iesGraph,subject=foldPart,predicate=ies.ipo,obj=followedURI)
    if (inferenceSystem):
        pw = ies.instantiate(iesGraph=iesGraph,_class=ies.possibleWorld)
        ies.addToGraph(iesGraph=iesGraph,subject=fol,predicate=ies.ipao,obj=pw)
        assessment = ies.instantiate(iesGraph=iesGraph,_class=ies.assess)
        ies.addToGraph(iesGraph=iesGraph,subject=assessment,predicate=ies.ass,obj=pw)
        assr = instantiate(iesGraph=iesGraph,_class=ies.assessor)
        ies.addToGraph(iesGraph=iesGraph,subject=assr,predicate=ies.ipi,obj=assessment)
        ies.addToGraph(iesGraph=iesGraph,subject=assr,predicate=ies.ipo,obj=inferenceSystem)
        #Now put the assessment in a period - i.e. when the assessment was done
        ies.putInPeriod(iesGraph=iesGraph,item=assessment,iso8601TimeString="2007-01-02T09:17:04")

#Now say one is following the other (they're not, but I didn't have any data where ships followed each other)
#First we create an object for the system that detected it. 
hal = instantiate(iesGraph=graph,_class=system)
addName(iesGraph=graph,item=hal,nameString="HAL")
#now create the following pattern, with our system included as the assessor
following(iesGraph=graph,followerMMSI="367000150",followedMMSI="366952890",startTimeStamp="2007-01-01T00:00:09",endTimeStamp="2007-01-01T00:05:40",inferenceSystem=hal)

saveRdf(graph,'output.ies.ttl')