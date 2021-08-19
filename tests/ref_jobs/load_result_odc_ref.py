from dask.distributed import Client
import datacube
import openeo_processes as oeop

# Initialize ODC instance
cube = datacube.Datacube(app='collection', env='default')
cube_user_gen = datacube.Datacube(app='user_gen', env='user_generated')
# Connect to Dask Scheduler
client = Client('tcp://xx.yyy.zz.kk:8786')


def extra_func_23_0(data, *parameters):
    _yn35xe0dv_8 = oeop.pi(**{})
    _05070e1r8_17 = oeop.pi(**{})
    _xlvludp52_6 = oeop.multiply(**{'x': 2, 'y': _yn35xe0dv_8})
    _yacyqslpk_16 = oeop.multiply(**{'x': 2, 'y': _05070e1r8_17})
    _avbce9bsn_5 = oeop.array_element(**{'data': parameters, 'index': 0})
    _9u1th2y7s_13 = oeop.array_element(**{'data': parameters, 'index': 1})
    _gdtbgtuga_15 = oeop.array_element(**{'data': parameters, 'index': 2})
    _lhle6zhbs_4 = oeop.divide(**{'x': _xlvludp52_6, 'y': 31557600})
    _jkwmntgs9_9 = oeop.divide(**{'x': _yacyqslpk_16, 'y': 31557600})
    _b6xvzbzzu_3 = oeop.multiply(**{'x': _lhle6zhbs_4, 'y': data})
    _bvmn4kpmp_12 = oeop.multiply(**{'x': _jkwmntgs9_9, 'y': data})
    _o1hqxg6zy_14 = oeop.cos(**{'x': _b6xvzbzzu_3})
    _6lw05bkrw_1 = oeop.sin(**{'x': _bvmn4kpmp_12})
    _bf91wdpfm_7 = oeop.multiply(**{'x': _9u1th2y7s_13, 'y': _o1hqxg6zy_14})
    _3mlkowiei_2 = oeop.multiply(**{'x': _gdtbgtuga_15, 'y': _6lw05bkrw_1})
    _pjq0xlzdd_11 = oeop.add(**{'x': _avbce9bsn_5, 'y': _bf91wdpfm_7})
    _w3t4ee2dc_10 = oeop.add(**{'x': _pjq0xlzdd_11, 'y': _3mlkowiei_2})
    return _w3t4ee2dc_10


_28_18 = oeop.load_collection(odc_cube=cube, **{'product': 'boa_sentinel_2', 'dask_chunks': {'time': 'auto', 'x': 1000, 'y': 1000}, 'x': (11.3679141998291, 11.535799026489256), 'y': (46.29756750209694, 46.394735677983675), 'time': ['2016-09-01T00:00:00Z', '2018-08-31T23:59:59Z'], 'measurements': ['B04', 'B02', 'B08', 'B03', 'B8A']})

_load_result1_19 = oeop.load_result(odc_cube=cube_user_gen, **{'product': '54dad8f8_9776_4373_8474_e2b940604d44', 'dask_chunks': {'time': 'auto', 'x': 1000, 'y': 1000}})
_23_0 = oeop.predict_curve(**{'data': _28_18, 'function': extra_func_23_0, 'parameters': _load_result1_19, 'dimension': 't', 'labels': []})
_saveresult1_20 = oeop.save_result(**{'data': _23_0, 'format': 'NETCDF'})
