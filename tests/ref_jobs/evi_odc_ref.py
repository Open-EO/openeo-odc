from dask.distributed import Client
import datacube
import openeo_processes as oeop

# Initialize ODC instance
cube = datacube.Datacube(app='collection', env='default')
cube_user_gen = datacube.Datacube(app='user_gen', env='user_generated')
# Connect to Dask Scheduler
client = Client('tcp://xx.yyy.zz.kk:8786')

_dc_0 = oeop.load_collection(odc_cube=cube, **{'product': 'boa_sentinel_2', 'dask_chunks': {'time': 'auto', 'x': 1000, 'y': 1000}, 'x': (9.9, 10.0), 'y': (46.5, 46.6), 'time': ['2018-06-15T00:00:00Z', '2018-06-16T00:00:00Z'], 'measurements': ['B08', 'B04', 'B02']})
_nir_2 = oeop.array_element(**{'data': _dc_0, 'index': 0, 'dimension': 'bands'})
_red_3 = oeop.array_element(**{'data': _dc_0, 'index': 1, 'dimension': 'bands'})
_blue_4 = oeop.array_element(**{'data': _dc_0, 'index': 2, 'dimension': 'bands'})
_sub_5 = oeop.subtract(**{'x': _nir_2, 'y': _red_3})
_p1_6 = oeop.multiply(**{'x': _red_3, 'y': 6})
_p2_7 = oeop.multiply(**{'x': _blue_4, 'y': -7.5})
_sum_8 = oeop.sum(**{'data': [10000, _nir_2, _p1_6, _p2_7]})
_div_9 = oeop.divide(**{'x': _sub_5, 'y': _sum_8})
_p3_10 = oeop.multiply(**{'x': _div_9, 'y': 2.5})
_evi_1 = oeop.reduce_dimension(**{'data': _p3_10, 'dimension': 'spectral', 'reducer': {}})
_min_12 = oeop.min(**{'data': _evi_1, 'dimension': 'time'})
_mintime_11 = oeop.reduce_dimension(**{'data': _min_12, 'dimension': 'temporal', 'reducer': {}})
_save_13 = oeop.save_result(**{'data': _mintime_11, 'format': 'netCDF'})
