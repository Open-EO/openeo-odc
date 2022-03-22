from dask_gateway import Gateway
import datacube
import openeo_processes as oeop
import time

# Initialize ODC instance
cube = datacube.Datacube(app='collection', env='default')
cube_user_gen = datacube.Datacube(app='user_gen', env='user_generated')
# Connect to the gateway
gateway = Gateway('tcp://xx.yyy.zz.kk:8786')
options = gateway.cluster_options()
options.user_id = 'test-user'
options.job_id = 'test-job'
cluster = gateway.new_cluster(options)
cluster.adapt(minimum=1, maximum=3)
time.sleep(60)
client = cluster.get_client()

_loadcollection1_0 = oeop.load_collection(odc_cube=cube, **{'product': 'S2_L2A_T32TPS', 'dask_chunks': {'y': 12160, 'x': 12114, 'time': 'auto'}, 'x': (10.960229020571205, 10.975120481571418), 'y': (45.91379959511596, 45.920009625521885), 'time': ['2017-07-01T00:00:00Z', '2017-07-07T23:59:59Z'], 'measurements': ['B04_10m', 'B03_10m', 'B02_10m']})
_min1_2 = oeop.min(**{'data': _loadcollection1_0, 'dimension': 'time'})
_reducedimension2_1 = oeop.reduce_dimension(**{'data': _min1_2, 'dimension': 't', 'reducer': {}})
_sqrt1_4 = oeop.sqrt(**{'x': _reducedimension2_1})
_apply1_3 = oeop.apply(**{'data': _sqrt1_4, 'process': _sqrt1_4, 'context': ''})
_saveresult1_5 = oeop.save_result(**{'data': _apply1_3, 'format': 'GTiff', 'options': {}})
cluster.shutdown()
gateway.close()