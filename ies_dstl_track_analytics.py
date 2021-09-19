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
    return cooperEvent


def testCooper():
    ies.setDataUri(dataUri) #Make sure that any auto-generated IDs follow the URI stub we're using. 

    mmsis = ['234234000','235083854','356793000']
    startTime="2021-08-11T10:47:48"
    endTime="2021-08-11T14:01:22"
    vesselSTs = ["2021-08-11T10:47:48","2021-08-11T10:47:48","2021-08-11T11:18:15"] #The third vessel joins late
    vesselETs = ["2021-08-11T14:01:22","2021-08-11T12:50:41","2021-08-11T14:01:22"] #The second vessel leaves early

    cooper = cooperingAtSea(iesGraph,startTime,endTime,mmsis,vesselSTs,vesselETs)
    ies.assessPwProbability(iesGraph,[cooper],TrackAnalytics,0.3,True,"2021-08-12T09:00:00") #Add the probability of the coopering

    ies.saveRdf(iesGraph,"track-analytics.ttl")

#predictedDestination expects an array of arrays (list of lists). Each inner list contains [portName,un_locode,iso3166ThreeLetterCountryCode,probability]
def predictedDestination(iesGraph,vesselMMSI,destinations,assessmentDT):
    vessel = ies.createIdentifiedEntity(iesGraph,ies.vessel,URIRef(dataUri+"_vessel_mmsi_"+vesselMMSI),idClass=ies.commsIdentifier,namingScheme=ies.mmsiNs)

    for destination in destinations:
        portName = destination[0]
        un_locode = destination[1]
        iso3166ThreeLetterCountryCode = destination[2]
        probability = destination[3]
        if un_locode != "":  #If there's a locode for the Port, then let's make sure we have a consistent URI for it too !
            portUri = URIRef("http://www.un.org/locode#"+un_locode.replace(" ",""))
        else:
            portUri = None
        port = ies.instantiate(iesGraph,ies.port,portUri)
        ies.addName(iesGraph,port,portName,ies.placeName)
        if un_locode != "":
            ies.addIdentifier(iesGraph,port,un_locode,idClass=ies.unLocode)
        if iso3166ThreeLetterCountryCode != "": #If the country has been specified, then create an appropriate URI for the country
            country = ies.instantiate(iesGraph,ies.country,URIRef("http://iso.org/iso3166-alpha3#"+iso3166ThreeLetterCountryCode))
            ies.addIdentifier(iesGraph,country,iso3166ThreeLetterCountryCode,idClass=ies.iso3166A3,idUri = URIRef("http://iso.org/iso3166-alpha3#"+iso3166ThreeLetterCountryCode+"_code"))
        ies.addToGraph(iesGraph,port,ies.ipao,country)
        sailing = ies.instantiate(iesGraph,ies.sailing)
        vu = ies.instantiate(iesGraph,ies.vehicleUsed)
        ies.addToGraph(iesGraph,vu,ies.ipo,vessel)
        ies.addToGraph(iesGraph,vu,ies.ipi,sailing)
        arrival = ies.instantiate(iesGraph,ies.arrival)
        ies.addToGraph(iesGraph,arrival,ies.ieo,sailing)
        ies.addToGraph(iesGraph,arrival,ies.il,port)
        ies.assessPwProbability(iesGraph,[sailing,vu,arrival],TrackAnalytics,probability,True,assessmentDT) #Add the probability of the coopering


def testDestination():
    predictedDestination(iesGraph,'234234000',[["Pontefract","GB POF","GBR",0.5],["Hull","GB HUL","GBR",0.3],["Ackworth","GB AMT ","GBR",0.2]],"2021-08-12T09:00:00")
    ies.saveRdf(iesGraph,"ta-pred-dest.ttl")


iesGraph = ies.initialiseGraph()
TrackAnalytics = ies.createSystem(iesGraph,"TrackAnalytics",URIRef("http://trackanalytics.dstl.gov.uk/TrackAnalytics"))
#testCooper()
testDestination()