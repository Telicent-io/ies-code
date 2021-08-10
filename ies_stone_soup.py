import ies_functions as ies
from rdflib import Graph, plugin, URIRef, BNode, Literal
dataUri="http://stonesoup.dstl.gov.uk/"

def exportStoneSoupTrack(iesGraph,trackId,states,mapping,targetId,targetIdType=ies.commsIdentifier,targetIdNamingScheme=ies.mmsiNs,epsgCode="4326"):
    minDT = None
    maxDT = None
    ies.setDataUri(dataUri)
    #Create the observation for the whole track
    obs = ies.instantiate(iesGraph=iesGraph,_class=ies.observation,instance=URIRef(dataUri+trackId))
    #Create the transponder (we may already have this)
    if targetIdNamingScheme != ies.mmsiNs:
        transponderUri = URIRef(dataUri+"_transponderID_"+targetId)
    else:
        transponderUri = URIRef(dataUri+"_MMSI_"+targetId)
    transponder = ies.createIdentifiedEntity(iesGraph=iesGraph,entityClass=ies.locationTransponder,entityID=targetId,namingScheme=targetIdNamingScheme,uri=transponderUri)
    for state in states:
        #need to get max and min timestamp for each state so we can establish start and end of the track
        if minDT == None:
            minDT = state.timestamp
        elif state.timestamp < minDT:
            minDT = state.timestamp
        if maxDT == None:
            maxDT = state.timestamp
        elif state.timestamp > maxDT:
            maxDT = state.timestamp
        #Create the Location observation
        lo = ies.instantiate(iesGraph=iesGraph,_class=ies.locationObservation)
        if obs:
            ies.addToGraph(iesGraph=iesGraph,subject=lo,predicate=ies.ipao,obj=obs)
        #...and the ParticularPeriod in which the observation occurred
        ies.putInPeriod(iesGraph=iesGraph,item=lo,timeString=str(state.timestamp))
        ltPart = ies.instantiate(iesGraph=iesGraph,_class=ies.observedTarget)
        ies.addToGraph(iesGraph=iesGraph,subject=ltPart,predicate=ies.ipo,obj=transponder) #participation of the transponder
        ies.addToGraph(iesGraph=iesGraph,subject=ltPart,predicate=ies.ipi,obj=lo) #participation in the LocationObservation
        #In this example, we have UTM coordinates, but Stone Soup can pretty much send you anything. So...we used EPSG codes !
        gp = ies.instantiate(iesGraph=iesGraph,_class=ies.geoPoint)
        epRep = ies.instantiate(iesGraph=iesGraph,_class=ies.epsgRep)
        ies.addToGraph(iesGraph=iesGraph,subject=gp,predicate=ies.iib,obj=epRep)
        for i,measure in enumerate(state.state_vector[mapping, :]):
            epsgParam = ies.instantiate(iesGraph=iesGraph,_class=ies.epsgParams[i])
            ies.addToGraph(iesGraph=iesGraph,subject=epsgParam,predicate=ies.ir,obj=epRep)
            ies.addToGraph(iesGraph=iesGraph,subject=epsgParam,predicate=ies.rv,obj=Literal(str(measure)))
        gpPart = ies.instantiate(iesGraph=iesGraph,_class=ies.observedLocation)
        ies.addToGraph(iesGraph=iesGraph,subject=gpPart,predicate=ies.ipo,obj=gp) #participation of the GeoPoint
        ies.addToGraph(iesGraph=iesGraph,subject=gpPart,predicate=ies.ipi,obj=lo) #participation in the LocationObservation

        #print(state.state_vector[mapping, :])
        #print("-------------")
    ies.startsIn(iesGraph=iesGraph,item=obs,timeString=minDT.isoformat())
    ies.endsIn(iesGraph=iesGraph,item=obs,timeString=maxDT.isoformat())