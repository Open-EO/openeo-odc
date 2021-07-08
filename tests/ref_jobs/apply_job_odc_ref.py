from dask.distributed import Client
import datacube
import openeo_processes as oeop

# Initialize ODC instance
cube = datacube.Datacube(app='app_1', env='default')
# Connect to Dask Scheduler
client = Client('tcp://xx.yyy.zz.kk:8786')

loadcollection1_0 = oeop.load_collection(odc_cube=cube, **{'product': 'S2_L2A_T32TPS', 'dask_chunks': {'time': 'auto', 'x': 1000, 'y': 1000}, 'x': (10.960229020571205, 10.975120481571418), 'y': (45.91379959511596, 45.920009625521885), 'time': ['2017-07-01T00:00:00Z', '2017-07-07T23:59:59'], 'measurements': ['B04_10m', 'B03_10m', 'B02_10m']})
min1_2 = oeop.min(**{'data': loadcollection1_0, 'dimension': 'time'})
reducedimension2_1 = oeop.reduce_dimension(**{'data': min1_2, 'dimension': 't', 'reducer': {}})
sqrt1_4 = oeop.sqrt(**{'x': reducedimension2_1})
apply1_3 = oeop.apply(**{'data': sqrt1_4, 'process': {'from_node': 'sqrt1_4'}, 'context': ''})
saveresult1_5 = oeop.save_result(**{'data': apply1_3, 'format': 'GTiff', 'options': {}})
