from dask.distributed import Client
import datacube
import openeo_processes as oeop

# Initialize ODC instance
cube = datacube.Datacube(app='collection', env='default')
cube_user_gen = datacube.Datacube(app='user_gen', env='user_generated')
# Connect to Dask Scheduler
client = Client('tcp://xx.yyy.zz.kk:8786')


def extra_func_fitcurve1_3(data, *parameters):
    _multiply1_4 = oeop.multiply(**{'x': 1.991021277657232e-07, 'y': data})
    _multiply3_6 = oeop.multiply(**{'x': 1.991021277657232e-07, 'y': data})
    _arrayelement3_8 = oeop.array_element(**{'data': parameters, 'index': 2})
    _arrayelement2_9 = oeop.array_element(**{'data': parameters, 'index': 1})
    _arrayelement1_10 = oeop.array_element(**{'data': parameters, 'index': 0})
    _cos1_7 = oeop.cos(**{'x': _multiply1_4})
    _sin1_11 = oeop.sin(**{'x': _multiply3_6})
    _multiply2_5 = oeop.multiply(**{'x': _arrayelement2_9, 'y': _cos1_7})
    _multiply4_12 = oeop.multiply(**{'x': _arrayelement3_8, 'y': _sin1_11})
    _add1_14 = oeop.add(**{'x': _arrayelement1_10, 'y': _multiply2_5})
    _add2_13 = oeop.add(**{'x': _add1_14, 'y': _multiply4_12})
    return _add2_13


_loadcollection1_0 = oeop.load_collection(odc_cube=cube, **{'product': 'boa_sentinel_2', 'dask_chunks': {'time': 'auto', 'x': 1000, 'y': 1000}, 'x': (11.410299, 11.413905), 'y': (46.341515, 46.343144), 'time': ['2016-09-01', '2018-08-31'], 'measurements': ['B02', 'B03', 'B04', 'B05', 'B08']})
_clip1_2 = oeop.clip(**{'x': _loadcollection1_0, 'min': 0, 'max': 4000})
_apply2_1 = oeop.apply(**{'process': _clip1_2, 'data': _clip1_2})
_fitcurve1_3 = oeop.fit_curve(**{'data': _apply2_1, 'function': extra_func_fitcurve1_3, 'parameters': [1, 1, 1], 'dimension': 't'})
_saveresult1_15 = oeop.save_result(**{'data': _fitcurve1_3, 'format': 'NetCDF', 'options': {}})
