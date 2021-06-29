from dask.distributed import Client
import datacube
import openeo_processes as oeop

# Initialize ODC instance
cube = datacube.Datacube(app='app_1', env='default')
# Connect to Dask Scheduler
client = Client('tcp://xx.yyy.zz.kk:8786')

dc_0 = oeop.load_collection(odc_cube=cube, **{'product': 'boa_sentinel_2', 'dask_chunks': {'time': 'auto', 'x': 1000, 'y': 1000}, 'x': (9.9, 10.0), 'y': (46.5, 46.6), 'time': ['2018-06-15', '2018-06-16'], 'measurements': ['B08', 'B04', 'B02']})
nir_2 = oeop.array_element(**{'data': dc_0, 'index': 0, 'dimension': 'bands'})
red_3 = oeop.array_element(**{'data': dc_0, 'index': 1, 'dimension': 'bands'})
blue_4 = oeop.array_element(**{'data': dc_0, 'index': 2, 'dimension': 'bands'})
sub_5 = oeop.subtract(**{'x': nir_2,'y': red_3})
p1_6 = oeop.multiply(**{'x': red_3,'y': 6})
p2_7 = oeop.multiply(**{'x': blue_4,'y': -7.5})
sum_8 = oeop.sum(**{'data': [10000, nir_2, p1_6, p2_7]})
div_9 = oeop.divide(**{'x': sub_5,'y': sum_8})
p3_10 = oeop.multiply(**{'x': div_9,'y': 2.5})
evi_1 = oeop.reduce_dimension(**{'data': p3_10, 'dimension': 'spectral', 'reducer': {}})
min_12 = oeop.min(**{'data': evi_1, 'dimension': 'time'})
mintime_11 = oeop.reduce_dimension(**{'data': min_12, 'dimension': 'temporal', 'reducer': {}})
save_13 = oeop.save_result(**{'data': mintime_11, 'format': 'netCDF'})
