import pickle
import ies_stone_soup as iesss
import ies_functions as ies
import stonesoup
import numpy as np

from stonesoup.models.measurement.linear import LinearGaussian
measurement_model = LinearGaussian(
    ndim_state=4, mapping=[0, 2], noise_covar=np.diag([15, 15]))

file = open("./ss.pkl","rb")
tracks = pickle.load(file)

iesGraph = ies.initialiseGraph()



for track in tracks:
    print(track.metadata)
    iesss.exportStoneSoupTrack(iesGraph,track.id,track.states,measurement_model.mapping,targetId=track.metadata["MMSI"],epsgCode=32630)
    ies.saveRdf(iesGraph,"./ss.ttl")
    quit()

print(iesGraph.serialize(format='ttl', indent=4).decode())