from rdflib import Graph, plugin, URIRef, BNode, Literal
from rdflib.namespace import DC, DCAT, DCTERMS, OWL, RDF, RDFS, XMLNS, XSD
from rdflib.serializer import Serializer






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