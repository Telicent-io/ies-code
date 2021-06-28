import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import dash_table
import dash_bootstrap_components as dbc
import dash_daq as daq
from kafka import KafkaConsumer, KafkaProducer
import json
import time
import zlib
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import uuid

import datetime
import plotly.express as px
import pandas as pd
import plotly.graph_objects as go
import threading
import os, sys
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
import ies_functions as ies

#This is a (relatively) simple app for monitoring RDF data moving through a KAFKA log. Because of the throughput of Kafka, this app works in three threads:
        #Thread one - watchKafka() - takes every message coming through the log and grabs summary data about it...but no calculations
        #Thread two - summariseData() - takes the data from thread one and carries out summarisation tasks, then stores the data ready for thread 3
        #Thread three - updateCache() - periodically checks the summarised data and pushes to the client-side cache, the HTML components read it from there.


bright_colours = ['#009fff',  # neon blue
                 '#ff7f0e',  # safety orange
                 '#e377c2',  # raspberry yogurt pink
                 '#39ff14',  # lime green
                 '#00ffdf',  # cyan
                 '#ffff00',  # bright yellow
                 '#d62728',  # brick red
                 '#ab20fd',  # neon purple
                 '#17becf',  # blue-teal
                 '#7f7f7f',  # middle gray
                '#009fff',  # neon blue
                 '#ff7f0e',  # safety orange
                 '#e377c2',  # raspberry yogurt pink
                 '#39ff14',  # lime green
                 '#00ffdf',  # cyan
                 '#ffff00',  # bright yellow
                 '#d62728',  # brick red
                 '#ab20fd',  # neon purple
                 '#17becf',  # blue-teal
                 '#7f7f7f',  # middle gray
                '#009fff',  # neon blue
                 '#ff7f0e',  # safety orange
                 '#e377c2',  # raspberry yogurt pink
                 '#39ff14',  # lime green
                 '#00ffdf',  # cyan
                 '#ffff00',  # bright yellow
                 '#d62728',  # brick red
                 '#ab20fd',  # neon purple
                 '#17becf',  # blue-teal
                 '#7f7f7f'  # middle gray

                 ]

broker = os.getenv("BOOTSTRAP_SERVERS")
iesTopic = os.getenv("IES_TOPIC")
aisTopic = os.getenv("AIS_TOPIC")
dataNamespace = os.getenv("IES_NAMESPACE")
iesNamespace = os.getenv("IES_NAMESPACE")

consumer = KafkaConsumer(iesTopic)

appStarted = datetime.datetime.now()

kafkaCache = []
monitorDict = {"traces":{},"timeStamps":[],"objCounts":[],"tripCounts":[],"totalObjectCount":0,"totalTripCount":0,"msgCount":0,"objRate":0,"tripRate":0,"msgRate":0}
sampleSize = 5 #Dictates the number of timeStamps, objCounts, etc. on which the averages are calculated
namespaces = {}
graph=ies.initialiseGraph(None)

graphLayout = go.Layout(
        template="plotly_dark",height=900
    )


#watchKafka - background thread with loop driven from the kafka consumer
            # reads the message into rdflib, counts the triples, counts the objects (things with rdf:type predicates)
            # if it's an object, it also adds a dictionary entry for each type and counts the instances of that type
def watchKafka():
    global kafkaCache
    global graph
    for msg in consumer:
        objCount = 0
        tripCount = 0
        newTrace = {"timeStamp":datetime.datetime.now(),"items":{}}
        zipNT = msg.value
        binNT = zlib.decompress(zipNT)
        nt = binNT.decode('utf8')
        ies.parseNT(graph,nt)
        for subj, pred, obj in graph:
            tripCount = tripCount + 1
            if pred.toPython() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type":
                objCount = objCount + 1
                if obj.toPython() in newTrace["items"]:
                    newTrace["items"][obj.toPython()] = newTrace["items"][obj.toPython()] + 1
                else:
                    newTrace["items"][obj.toPython()] = 1
        newTrace["tripCount"] = tripCount
        newTrace["objCount"] = objCount
        kafkaCache.append(newTrace)

#summariseData - another thread !  This one periodically checks the kafkaCache global dict (which is populated by the watchKafka function running on another thread...I hate threads)
            #This function outputs to another dictionary monitorDict (I was trying to be careful to keep all of this thread-safe...could possible get away with same dict, but nobody wants to be debugging that)
def summariseData():
    threading.Timer(0.2, summariseData).start()
    global kafkaCache
    global monitorDict
    global sampleSize
    while len(kafkaCache) > 0:
        #monitorDict = {"traces":{},"timeStamps":[],"objCounts":[],"tripCounts":[],"totalObjectCount":0,"totalTripCount":0,"msgCount":0,"objRate":0,"tripRate":0,"msgRate":0}
        trace = kafkaCache.pop(0)
        monitorDict["msgCount"] = monitorDict["msgCount"] + 1
        monitorDict["totalTripCount"] = monitorDict["totalTripCount"] + trace["tripCount"]
        monitorDict["totalObjectCount"] = monitorDict["totalObjectCount"] + trace["objCount"]
        monitorDict["timeStamps"].append(trace["timeStamp"])
        while len(monitorDict["timeStamps"]) > sampleSize:
            monitorDict["timeStamps"].pop(0)
        monitorDict["objCounts"].append(trace["objCount"])
        while len(monitorDict["objCounts"]) > sampleSize:
            monitorDict["objCounts"].pop(0)
        monitorDict["tripCounts"].append(trace["tripCount"])
        while len(monitorDict["tripCounts"]) > sampleSize:
            monitorDict["tripCounts"].pop(0)
        #print("msgRate:",monitorDict["msgRate"],"tripRate:",monitorDict["tripRate"],"objRate:",monitorDict["objRate"])
        #All high-level summary lists updated. Now calculate rates
        if len(monitorDict["timeStamps"]) > 1:
            minTS = monitorDict["timeStamps"][0]
            maxTS = monitorDict["timeStamps"][len(monitorDict["timeStamps"])-1]
            timeDiff = (maxTS-minTS).total_seconds()    
            monitorDict["objRate"] = sum(monitorDict["objCounts"])/timeDiff
            monitorDict["tripRate"] = sum(monitorDict["tripCounts"])/timeDiff
            monitorDict["msgRate"] = len(monitorDict["timeStamps"])/timeDiff
        for key in trace["items"]:
            item = trace["items"][key]
            if not key in monitorDict["traces"]:
                monitorDict["traces"][key] = {"totalCount":0,"timeStamps":[],"counts":[],"rate":0}
            monitorDict["traces"][key]["totalCount"] = monitorDict["traces"][key]["totalCount"] + item
            monitorDict["traces"][key]["timeStamps"].append(trace["timeStamp"])
            monitorDict["traces"][key]["counts"].append(item)
            while len(monitorDict["traces"][key]["counts"]) > sampleSize:
                monitorDict["traces"][key]["counts"].pop(0)
            while len(monitorDict["traces"][key]["timeStamps"]) > sampleSize:
                monitorDict["traces"][key]["timeStamps"].pop(0)
            if len(monitorDict["traces"][key]["timeStamps"]) > 1:
                minTS = monitorDict["traces"][key]["timeStamps"][0]
                maxTS = monitorDict["traces"][key]["timeStamps"][len(monitorDict["traces"][key]["timeStamps"])-1]
                timeDiff = (maxTS-minTS).total_seconds()
                msgCount = sum(monitorDict["traces"][key]["counts"])
                monitorDict["traces"][key]["rate"] = msgCount / timeDiff



app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])


@app.callback(Output('cache', 'data'),
              Input('interval-component', 'n_intervals'),
              Input('switches-input',"value"),
              State('switches-input',"options"),
              State('cache', 'data'))
def updateCache(n,swValue,swOptions,data):
    global monitorDict
    global appStarted
    global graph
    ctx = dash.callback_context.triggered[0]['prop_id']
    if ctx == "interval-component.n_intervals":
        #monitorDict = {"traces":{},"timeStamps":[],"objCounts":[],"tripCounts":[],"totalObjectCount":0,"totalTripCount":0,"msgCount":0,"objRate":0,"tripRate":0,"msgRate":0}
        if data == {}:
            data = {"totalTriples":0,"totalMessages":0,"totalObjects":0,"tripleRate":0,"msgRate":0,"averageObjectRate":0,"averageTripleRate":0,"averageMsgRate":0,"monitoringStarted":appStarted,"lastTimestamp":appStarted,"traces":{}}
        data["totalMessages"] = monitorDict["msgCount"]
        data["totalObjects"] = monitorDict["totalObjectCount"]
        data["totalTriples"] = monitorDict["totalTripCount"]
        data["averageTripleRate"] = monitorDict["tripRate"]
        data["averageMsgRate"] = monitorDict["msgRate"]
        data["averageObjectRate"] = monitorDict["objRate"]
        if not "traces" in data:
            data["traces"] = {}
        for key in monitorDict["traces"]:
            if not key in data:
                ns = graph.namespace_manager.compute_qname(key)
                data["traces"][key] = {"shortUri":ies.getShortUri(graph,key),"namespace":ns[0],"shortName":ns[0]+":"+ns[2]}
            for item in monitorDict["traces"][key]:
                data["traces"][key][item] = monitorDict["traces"][key][item]
            #http://myOntology.org.uk/classes#OffshoreSupplyVessel
            #{'totalCount': 6, 'timeStamps': [datetime.datetime(2021, 6, 25, 17, 53, 51, 176709), datetime.datetime(2021, 6, 25, 17, 53, 52, 968885), datetime.datetime(2021, 6, 25, 17, 53, 53, 379539), datetime.datetime(2021, 6, 25, 17, 53, 54, 5048), datetime.datetime(2021, 6, 25, 17, 53, 54, 426163)], 'counts': [1, 1, 1, 1, 1], 'rate': 1.5387200434288344}  

    if False:
        data["totalMessages"] = monitorDict["msgCount"]
        while len(monitorDict["traces"]) > 0:
            trace = monitorDict["traces"][0]
            if data == {}:
                data = {"totalTriples":0,"totalMessages":0,"totalObjects":0,"tripleRate":0,"msgRate":0,"averageObjectRate":0,"averageTripleRate":0,"averageMsgRate":0,"monitoringStarted":appStarted,"lastTimestamp":appStarted,"traces":{}}
            data["totalTriples"] = data["totalTriples"]+trace["tripCount"]
            data["totalObjects"] = data["totalObjects"]+trace["objCount"]
            data["totalMessages"] = data["totalMessages"]+1
            timeDiff = (trace["timeStamp"] - data["lastTimeStamp"]).total_seconds()
            data["averageTripleRate"] = trace["tripCount"]/timeDiff
            data["averageMsgRate"] = 1/timeDiff
            data["averageObjectRate"] = trace["objCount"]/timeDiff
            data["lastTimestamp"] = trace["timeStamp"]
            for key in trace["items"]:
                item = trace["items"][key]
                if not key in data:
                    ns = ies.graph.namespace_manager.compute_qname(key)
                    data[key] = {"shortUri":ies.getShortUri(graph,key),"namespace":ns[0],"shortName":ns[0]+":"+ns[2],"counts":[],"timestamps":[],"showInBarChart":True,"rate":0}
                data[key]["counts"].append(trace[key])
                if len(data[key]["counts"]) > 5:
                    data[key]["counts"].pop(0)
                data[key]["timestamps"].append(trace["timeStamp"])
                if len(data[key]["timestamps"]) > 5:
                    data[key]["timestamps"].pop(0)
            monitorDict["traces"].pop(0)
    return data

@app.callback(Output('switches-input', 'options'),
              Input('cache', 'data'))
def updateMetrics(data):
    children = []
    options = []

    i = 0
    if data != {}:
        keys = sorted(data["traces"].keys(),reverse=True)
        for key in keys:
            options.append({"label":data["traces"][key]["shortName"]+" ("+str(data["traces"][key]["totalCount"])+")","value":key})
            i = i + 1
    return options

@app.callback(
    Output("msg-rate-gauge", "value"), 
    Input('cache', 'data'))
def updateMsgRate(data):
    if data != {}:
        return data["averageMsgRate"]

@app.callback(
    Output("trip-rate-gauge", "value"), 
    Input('cache', 'data'))
def updateTripRate(data):
    if data != {}:
        return data["averageTripleRate"]

@app.callback(
    Output("obj-rate-gauge", "value"), 
    Input('cache', 'data'))
def updateObjRate(data):
    if data != {}:
        return data["averageObjectRate"]

@app.callback(
    Output("bar-chart", "figure"), 
    Input('cache', 'data'),
    State('switches-input', 'options'),
    State('switches-input', 'value'),
    State("bar-chart", "figure"))
def updateBarChart(data,options,value,oldFig):
    myList = []
    if data != {}:
        #print(json.dumps(data))
        for key in data["traces"]:
            item = data["traces"][key]
            if key in value:
                text = "rate="+str(round(item["rate"],2))+" count="+str(item["totalCount"])
                myList.append([key,item["shortName"],text,str(item["namespace"]),item["rate"]])
        df = pd.DataFrame.from_records(myList,columns=["class","shortName","count","namespace",'rate']).sort_values(by=['shortName'])

        fig = go.Figure(data=[go.Bar(y=df["shortName"],x=df["rate"],textposition='auto',text=df["count"],name=str(df["namespace"]),orientation="h",marker_color=bright_colours)],layout=graphLayout)
    else:
        fig = go.Figure(data=[go.Bar(orientation="h",marker_color=bright_colours)],layout=graphLayout)
    #fig = px.bar(df, y="class", x="count", orientation='h', height=600, layout=layout)
    return fig








def make_layout():
    chart_title = "data updates server-side every 5 seconds"
    return html.Div(
        children=[
            dcc.Interval(
            id='interval-component',
            interval=2000, # in milliseconds
            n_intervals=0
            ),
            dcc.Store(id='cache', storage_type='memory',data={}),
            html.Div(
                className='row telicent-header',
                children=[
                    html.Div(
                        className='col-md-4 logo-div',
                        children=[
                            html.P("MONITOR", className="app-title"),
                        ]
                    ),
                    html.Div(
                        className='col-md-4 app-title',
                        children=[
                            html.Img(src=app.get_asset_url('limegreen-logo.svg'),width="50px",className="logo"),
                            
                        ]
                    ),
                    html.Div(
                        className='col-md-4 app-controls',
                        children=[
                            html.Span("menu", id="menu-button",className="material-icons icon-button", title="menu"),
                        ]
                    )
                ]
            ),
            html.Div(id="app-body",className="row", style={"marginTop":"15px"},children=            [
                html.Div(id="controls-pane",className="col-md-2",children=[
                    html.Div(id="side",children=[
                        dbc.Checklist(
                            options=[],
                            value=[],
                            id="switches-input",
                            switch=True,
                            style={"marginTop":"100px"}
                        )
                    ],style={"fontSize":"0.6em"})
                ]
                ),
                html.Div(id="main-pane",className="col-md-10",children=[
                    dcc.Graph(id="bar-chart"),

                ]
                ),
            ]
            ),
            html.Div(id="spaedos",className="row",children=[
                html.Div(id="left-of-gauges",className="col-md-3"),
                html.Div(id="gauges-col",className="col-md-9", children=[
                    html.Div(id="gauges",className="row",children= [
                        daq.Gauge(
                            id='msg-rate-gauge',
                            label="Message Rate /S",
                            value=0,
                            max=20,
                        ),
                        daq.Gauge(
                            id='trip-rate-gauge',
                            label="Triple Rate /S",
                            value=0,
                            max=3000
                        ),
                        daq.Gauge(
                            id='obj-rate-gauge',
                            label="Object Rate /S",
                            value=0,
                            max=1400
                        )
                    ])
                ])
                
            ])

        ], className=""
            
    )


app.layout = make_layout

summariseData()

executor = ThreadPoolExecutor(max_workers=1)
executor.submit(watchKafka)



if __name__ == '__main__':
    app.run_server(debug=True, port=8052)

    