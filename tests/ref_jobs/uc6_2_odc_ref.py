from dask.distributed import Client
import datacube
import openeo_processes as oeop

# Initialize ODC instance
cube = datacube.Datacube(app='collection', env='default')
cube_user_gen = datacube.Datacube(app='user_gen', env='user_generated')
# Connect to Dask Scheduler
client = Client('tcp://xx.yyy.zz.kk:8786')


def extra_func_fitcurve1_9(x, *parameters):
    _arrayelement1_12 = oeop.array_element(**{'data': parameters, 'index': 0})
    _arrayelement2_13 = oeop.array_element(**{'data': parameters, 'index': 1})
    _arrayelement3_14 = oeop.array_element(**{'data': parameters, 'index': 2})
    _multiply1_16 = oeop.multiply(**{'x': 1.991021277657232e-07, 'y': x})
    _multiply3_18 = oeop.multiply(**{'x': 1.991021277657232e-07, 'y': x})
    _cos1_15 = oeop.cos(**{'x': _multiply1_16})
    _sin1_20 = oeop.sin(**{'x': _multiply3_18})
    _multiply2_17 = oeop.multiply(**{'x': _arrayelement2_13, 'y': _cos1_15})
    _multiply4_19 = oeop.multiply(**{'x': _arrayelement3_14, 'y': _sin1_20})
    _add1_10 = oeop.add(**{'x': _arrayelement1_12, 'y': _multiply2_17})
    _add2_11 = oeop.add(**{'x': _add1_10, 'y': _multiply4_19})
    return _add2_11


_1_0 = oeop.load_result(odc_cube=cube_user_gen, **{'product': 'jb_fa7a7188_c3ce_40fb_85d8_a5443ac112c3', 'dask_chunks': {'time': 'auto', 'x': 1000, 'y': 1000}})

_loadcollection1_5 = oeop.load_collection(odc_cube=cube, **{'product': 'boa_sentinel_2', 'dask_chunks': {'time': 'auto', 'x': 1000, 'y': 1000}, 'x': (11.410299, 11.413905), 'y': (46.341515, 46.343144), 'time': ['2016-09-01', '2018-08-31'], 'measurements': ['B02', 'B03', 'B04', 'B05', 'B08']})
_1_2 = oeop.eq(**{'x': _loadcollection1_5, 'y': 0})
_clip1_7 = oeop.clip(**{'x': _loadcollection1_5, 'max': 4000, 'min': 0})
_2_1 = oeop.apply(**{'data': _1_2, 'process': _1_2})
_apply2_6 = oeop.apply(**{'data': _clip1_7, 'process': _clip1_7})
_dimension_labels1_8 = oeop.dimension_labels(**{'data': _apply2_6, 'dimension': 't'})
_fitcurve1_9 = oeop.predict_curve(**{'data': _apply2_6, 'function': extra_func_fitcurve1_9, 'parameters': _1_0, 'labels': _dimension_labels1_8, 'dimension': 't'})
_1_4 = oeop.multiply(**{'x': _fitcurve1_9, 'y': _2_1})
_3_3 = oeop.merge_cubes(**{'cube1': _fitcurve1_9, 'cube2': _2_1, 'overlap_resolver': _1_4})
_saveresult1_21 = oeop.save_result(**{'data': _3_3, 'format': 'NetCDF', 'options': {}})
