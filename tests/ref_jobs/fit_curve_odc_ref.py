from dask.distributed import Client
import datacube
import openeo_processes as oeop

# Initialize ODC instance
cube = datacube.Datacube(app='app_1', env='default')
# Connect to Dask Scheduler
client = Client('tcp://xx.yyy.zz.kk:8786')


def fit_curve_func_18_0(x, *parameters):
    _32frj455b_1 = oeop.pi(**{})
    _2sjyaa699_11 = oeop.pi(**{})
    _lyjcuq5vd_15 = oeop.multiply(**{'x': 2, 'y': _32frj455b_1})
    _9k6vt7qcn_2 = oeop.multiply(**{'x': 2, 'y': _2sjyaa699_11})
    _1ipvki94n_4 = oeop.divide(**{'x': _lyjcuq5vd_15, 'y': 31557600})
    _p42lrxmbq_16 = oeop.divide(**{'x': _9k6vt7qcn_2, 'y': 31557600})
    _wz26aglyi_5 = oeop.multiply(**{'x': _p42lrxmbq_16, 'y': x})
    _kryhimf6r_6 = oeop.array_element(**{'data': parameters, 'index': 0})
    _jxs4umqsh_10 = oeop.array_element(**{'data': parameters, 'index': 1})
    _8jjjztmya_12 = oeop.array_element(**{'data': parameters, 'index': 2})
    _ya3hbxpot_17 = oeop.multiply(**{'x': _1ipvki94n_4, 'y': x})
    _v81bsalku_7 = oeop.cos(**{'x': _wz26aglyi_5})
    _0p7xlqeyo_8 = oeop.sin(**{'x': _ya3hbxpot_17})
    _jhus2gz74_13 = oeop.multiply(**{'x': _jxs4umqsh_10, 'y': _v81bsalku_7})
    _0v09jn699_14 = oeop.multiply(**{'x': _8jjjztmya_12, 'y': _0p7xlqeyo_8})
    _xb4c1hk1f_9 = oeop.add(**{'x': _kryhimf6r_6, 'y': _jhus2gz74_13})
    _b4mf181yp_3 = oeop.add(**{'x': _xb4c1hk1f_9, 'y': _0v09jn699_14})
    return _b4mf181yp_3


_23_20 = oeop.load_collection(odc_cube=cube, **{'product': 'boa_sentinel_2', 'dask_chunks': {'time': 'auto', 'x': 1000, 'y': 1000}, 'x': (11.5381, 11.5381), 'y': (46.4868, 46.4868), 'time': ['2016-01-01T00:00:00Z', '2016-05-31T00:00:00Z'], 'measurements': []})
_1_19 = oeop.clip(**{'x': _23_20, 'min': 0, 'max': 4000})
_22_18 = oeop.apply(**{'process': _1_19, 'data': _1_19, 'context': ''})
_18_0 = oeop.fit_curve(**{'data': _22_18, 'function': fit_curve_func_18_0, 'parameters': [1, 1, 1], 'dimension': 't'})
_saveresult1_21 = oeop.save_result(**{'data': _18_0, 'format': 'NETCDF'})
