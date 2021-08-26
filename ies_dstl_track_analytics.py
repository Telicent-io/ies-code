import ies_functions as ies
import datetime as dt
from rdflib import Graph, plugin, URIRef, BNode, Literal
dataUri="http://trackanalytics.dstl.gov.uk/"


#Coopering at sea
#This function generates the IES RDF data for vessels that are coopering at sea
#The startTime and endTime parameters mark the beginning and end of the coopering activity
#The vesselMmsiList is a list of MMSI identifiers for each of the vessels engaged in the coopering activity
#The vesselStartTimeList and vesselEndTimeList are lists of the same length as vesselMmsiList
def cooperingAtSea(iesGraph,startTime,endTime,vesselMmsiList,vesselStartTimeList=[],vesselEndTimeList=[]):
    vessels = []
    cooperEvent = ies.instantiate(iesGraph,_class=ies.cooper)
    if startTime != '':
        ies.startsIn(iesGraph,cooperEvent,startTime)
    if endTime != '':
        ies.endsIn(iesGraph,cooperEvent,endTime)
    for i,mmsi in enumerate(vesselMmsiList):
        vessel = ies.createIdentifiedEntity(iesGraph,ies.vessel,URIRef(dataUri+"_vessel_mmsi_"+mmsi),idClass=ies.commsIdentifier,namingScheme=ies.mmsiNs)
        coopering = ies.instantiate(iesGraph,_class=ies.coopering)
        ies.addToGraph(iesGraph,coopering,ies.ipi,cooperEvent)
        ies.addToGraph(iesGraph,coopering,ies.ipo,vessel)
        if len(vesselStartTimeList) == len (vesselMmsiList) and len(vesselStartTimeList) > i:
            vst = vesselStartTimeList[i]
            if vst != None and vst !='':
                ies.startsIn(iesGraph,coopering,vst)
        if len(vesselEndTimeList) == len (vesselMmsiList) and len(vesselEndTimeList) > i:
            vet = vesselEndTimeList[i]
            if vet != None and vet !='':
                ies.endsIn(iesGraph,coopering,vet)







def testCooper():
    iesGraph = ies.initialiseGraph()
    ies.setDataUri(dataUri) #Make sure that any auto-generated IDs follow the URI stub we're using. 

    mmsis = ['234234000','235083854','356793000']
    startTime="2021-08-11T10:47:48"
    endTime="2021-08-11T14:01:22"
    vesselSTs = ["2021-08-11T10:47:48","2021-08-11T10:47:48","2021-08-11T11:18:15"] #The third vessel joins late
    vesselETs = ["2021-08-11T14:01:22","2021-08-11T12:50:41","2021-08-11T14:01:22"] #The second vessel leaves early

    cooperingAtSea(iesGraph,startTime,endTime,mmsis,vesselSTs,vesselETs)

    ies.saveRdf(iesGraph,"track-analytics.ttl")

testCooper()