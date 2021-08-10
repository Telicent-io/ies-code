import pickle
from kafka_soup import KafkaSoup

track = pickle.load(open("track.pkl","rb"))
ks = KafkaSoup("localhost:9092","ARSE")

ks.pushTrack(track,[0,2])



#print(json.dumps(encodeTrack(track,"32651",[0,2],xyVelocityIndeces=[1,3]),indent="  "))
