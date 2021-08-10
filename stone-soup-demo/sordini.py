from kafka import KafkaProducer
import zlib
import os
import json


class Sordini:
    def __init__(self, broker, topic, zip=True, errTopic="_print"):
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

    def err(errMessage):
        if self.errTopic == "_print":
            print("KAFKAESQUE ERROR (you know there's an error, but you don't know who made it, why you're here, or what that strange castle is)") 
            print("  - ".join(errMessage))
        else:
            #Probably ought to add a feature in future version to allow error messages to be JSON also, esp. if multiple providers write to the same error topic
            self.producer.send(self.errTopic, errMessage.encode())

    #This recursively converts an object to a dict. This is required as the __dict__ operator in Python doesn't decend the graph
    def todict(self,obj, classkey=None):
        if isinstance(obj, dict):
            data = {}
            for (k, v) in obj.items():
                data[k] = self.todict(v, classkey)
            return data
        elif hasattr(obj, "_ast"):
            return self.todict(obj._ast())
        elif hasattr(obj, "__iter__") and not isinstance(obj, str):
            return [self.todict(v, classkey) for v in obj]
        elif hasattr(obj, "__dict__"):
            data = dict([(key, self.todict(value, classkey)) 
                for key, value in obj.__dict__.items() 
                if not callable(value) and not key.startswith('_')])
            if classkey is not None and hasattr(obj, "__class__"):
                data[classkey] = obj.__class__.__name__
            return data
        else:
            return obj

    def dictify(self,obj):
        dic = None
        print(obj)
        if isinstance(obj,object):
            dic = obj.__dict__
        elif isinstance(obj,dict):
            dic = obj
        if dic is not None:
            for key in dic:
                prop = dic[key]
                if isinstance(prop,list):
                    for i,thing in enumerate(prop):
                        prop[i] = self.dictify(thing)
                else:
                    if isinstance(prop,dict) or isinstance(prop,object):
                        dic[key] = self.dictify(prop)
            return dic
        else:
            raise Exception("unknown type: "+type(obj))
             

    def push(self,kData):
        #Now need to decide whether we've been fed a string, a dict or and object and deal with them appropriately, as any good castle administrator would
        #Everything becomes a string before being fed onto Kafka - don't want any nasty unreadable binary structures going onto the log.
        kString = None
        if isinstance(kData,str):
            #a string is a string
            kString = kData
        elif isinstance(kData,object):
            #extract the object's properties into a dict
            kDict = self.todict(kData)
            #serialise to JSON
            kString = json.dumps(kDict)
        elif isInstance(kData,dict):
            #serialise to JSON
            kString = json.dumps(kDict)
        if kString is None:
            self.err("kData must be a string, an object or a dict, faceless bureaucrats reject all data of type ".join(str(type(kData))))
        else:
            #We're in business. Let's drop this data in the k-hole. 
            print(kString)
            if self.zip:
                self.producer.send(self.topic,kString.encode())
            else:
                self.producer.send(self.topic,zlib.compress(bytes(kString)))