import json
from kafka import KafkaConsumer, KafkaProducer

class KafkaSoup:
    def __init__(self, broker, topic, zip=True, errTopic="_print", defaultEpsgCode="4326"):
        if defaultEpsgCode is not None and defaultEpsgCode != "":
            self.epsgCode = defaultEpsgCode
        else:
            #sigh. Some muppet decided to override the default with nothing. Some people, eh? I don't know. Mumble. Grumble
            self.epsgCode = "4326"
            print("WARNING: default EPSG code has been overridden with nothing. Forcing it back to EPSG:4326, WGS84 GPS Lat Lon ")
        if broker is not None and broker != "":
            self.broker = broker
        else:
            raise Exception("Specified broker is not valid")
        if topic is not None and topic != "":
            self.topic = topic
        else:
            raise Exception("Specified topic is not valid")
        if zip is not None and isinstance(zip,bool):
            self.zip = zip
        else:
            raise Exception("zip parameter must be boolean or None")
        if errTopic == "":
            raise Exception('if errTopic is specified, it must be a valid topic, or "_print" to just report errors to stdout')
        else:
            self.errTopic = errTopic
        self.producer = KafkaProducer(bootstrap_servers=[self.broker])
    
    def err(self,errMessage):
        if self.errTopic == "_print":
            print("KAFKAESQUE ERROR (you know there's an error, but you don't know who made it, why you're here, or what that strange castle is)") 
            print("  - ".join(errMessage))
        else:
            #Probably ought to add a feature in future version to allow error messages to be JSON also, esp. if multiple providers write to the same error topic
            self.producer.send(self.errTopic, errMessage.encode())

    def encodeTrack(self,track,xyIndeces,epsgCode=None,zIndex=None,xyVelocityIndeces=None,zVelocityIndex=None):
        dTrack = {"metadata":track.metadata,"states":[]}
        for state in track:
            dState = {"timestamp":state.timestamp.isoformat(),"coordinates":[state.state_vector[xyIndeces[0]],state.state_vector[xyIndeces[1]]],"epsgCode":epsgCode}
            if zIndex is not None:
                dState["coordinates"].append(state.state_vector[zIndex])
            if xyVelocityIndeces is not None:
                dState["velocityVector"] = [state.state_vector[xyVelocityIndeces[0]],state.state_vector[xyVelocityIndeces[1]]]
                if zVelocityIndex is not None:
                    dState["velocityVector"].append(state.state_vector[zVelocityIndex])
            dTrack["states"].append(dState)

        return dTrack

    def pushThing(self,thing,kafkaTopic=None):
        if kafkaTopic is None:
            topic = self.topic
        else:
            topic = kafkaTopic

        kString = json.dumps(thing)
        print(self.zip)
        if self.zip:
            self.producer.send(self.topic,zlib.compress(bytes(kString)))
            print("zip!")
        else:
            self.producer.send(self.topic,kString.encode())
            


    def pushTrack(self,track,xyIndeces,kafkaTopic=None,errorTopic=None,epsgCode=None,zIndex=None,xyVelocityIndeces=None,zVelocityIndex=None):
        dTrack = self.encodeTrack(track,xyIndeces,epsgCode,zIndex,xyVelocityIndeces,zVelocityIndex)
        self.pushThing(dTrack,kafkaTopic)


