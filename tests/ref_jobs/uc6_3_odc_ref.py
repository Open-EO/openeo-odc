from dask.distributed import Client
import datacube
import openeo_processes as oeop

# Initialize ODC instance
cube = datacube.Datacube(app='collection', env='default')
cube_user_gen = datacube.Datacube(app='user_gen', env='user_generated')
# Connect to Dask Scheduler
client = Client('tcp://xx.yyy.zz.kk:8786')


def extra_func_predictcurve1_12(x, *parameters):
    _multiply1_13 = oeop.multiply(**{'x': 1.991021277657232e-07, 'y': x})
    _multiply3_15 = oeop.multiply(**{'x': 1.991021277657232e-07, 'y': x})
    _arrayelement3_17 = oeop.array_element(**{'data': parameters, 'index': 2})
    _arrayelement2_18 = oeop.array_element(**{'data': parameters, 'index': 1})
    _arrayelement1_19 = oeop.array_element(**{'data': parameters, 'index': 0})
    _cos1_16 = oeop.cos(**{'x': _multiply1_13})
    _sin1_20 = oeop.sin(**{'x': _multiply3_15})
    _multiply2_14 = oeop.multiply(**{'x': _arrayelement2_18, 'y': _cos1_16})
    _multiply4_21 = oeop.multiply(**{'x': _arrayelement3_17, 'y': _sin1_20})
    _add1_23 = oeop.add(**{'x': _arrayelement1_19, 'y': _multiply2_14})
    _add2_22 = oeop.add(**{'x': _add1_23, 'y': _multiply4_21})
    return _add2_22


_loadcollection1_5 = oeop.load_collection(odc_cube=cube, **{'product': 'boa_sentinel_2', 'dask_chunks': {'time': 'auto', 'x': 1000, 'y': 1000}, 'x': (11.410299, 11.413905), 'y': (46.341515, 46.343144), 'time': ['2018-09-02', '2018-12-30'], 'measurements': ['B01', 'B02', 'B03', 'B04', 'B08']})

_loadresult1_6 = oeop.load_result(odc_cube=cube_user_gen, **{'product': '241f60ca_7623_4a86_a3d7_a0199eb57233', 'dask_chunks': {'time': 'auto', 'x': 1000, 'y': 1000}})

_loadresult2_7 = oeop.load_result(odc_cube=cube_user_gen, **{'product': '7628c410_ce6a_416d_a1e1_45f184ddbc65', 'dask_chunks': {'time': 'auto', 'x': 1000, 'y': 1000}})
_1_0 = oeop.dimension_labels(**{'data': _loadcollection1_5, 'dimension': 't'})
_1_2 = oeop.eq(**{'x': _loadcollection1_5, 'y': 0})
_clip1_11 = oeop.clip(**{'x': _loadcollection1_5, 'min': 0, 'max': 4000})
_multiply5_9 = oeop.multiply(**{'x': _loadresult2_7, 'y': 3})
_2_1 = oeop.apply(**{'data': _1_2, 'process': _1_2})
_apply2_10 = oeop.apply(**{'process': _clip1_11, 'data': _clip1_11})
_apply3_8 = oeop.apply(**{'process': _multiply5_9, 'data': _multiply5_9})
_predictcurve1_12 = oeop.predict_curve(**{'data': _apply2_10, 'function': extra_func_predictcurve1_12, 'parameters': _loadresult1_6, 'labels': _1_0, 'dimension': 't'})
_1_4 = oeop.multiply(**{'x': _predictcurve1_12, 'y': _2_1})
_3_3 = oeop.merge_cubes(**{'cube2': _2_1, 'cube1': _predictcurve1_12, 'overlap_resolver': _1_4})
_subtract1_25 = oeop.subtract(**{'x': _apply2_10, 'y': _3_3})
_mergecubes1_24 = oeop.merge_cubes(**{'cube2': _3_3, 'cube1': _apply2_10, 'overlap_resolver': _subtract1_25})
_arrayelement8_29 = oeop.array_element(**{'data': _mergecubes1_24, 'index': 4, 'dimension': 'bands'})
_arrayelement7_30 = oeop.array_element(**{'data': _mergecubes1_24, 'index': 1, 'dimension': 'bands'})
_arrayelement4_31 = oeop.array_element(**{'data': _mergecubes1_24, 'index': 2, 'dimension': 'bands'})
_arrayelement6_33 = oeop.array_element(**{'data': _mergecubes1_24, 'index': 3, 'dimension': 'bands'})
_arrayelement5_34 = oeop.array_element(**{'data': _mergecubes1_24, 'index': 0, 'dimension': 'bands'})
_power5_27 = oeop.power(**{'p': 2, 'base': _arrayelement8_29})
_power4_38 = oeop.power(**{'p': 2, 'base': _arrayelement7_30})
_power1_39 = oeop.power(**{'p': 2, 'base': _arrayelement4_31})
_power3_37 = oeop.power(**{'p': 2, 'base': _arrayelement6_33})
_power2_41 = oeop.power(**{'p': 2, 'base': _arrayelement5_34})
_add3_42 = oeop.add(**{'x': _power1_39, 'y': _power2_41})
_add4_40 = oeop.add(**{'x': _add3_42, 'y': _power3_37})
_add5_36 = oeop.add(**{'x': _add4_40, 'y': _power4_38})
_add6_35 = oeop.add(**{'x': _add5_36, 'y': _power5_27})
_divide1_32 = oeop.divide(**{'x': _add6_35, 'y': 5})
_power6_28 = oeop.power(**{'p': 0.5, 'base': _divide1_32})
_reducedimension1_26 = oeop.reduce_dimension(**{'data': _power6_28, 'reducer': {}, 'dimension': 'bands'})
_gt2_44 = oeop.gt(**{'x': _reducedimension1_26, 'y': _apply3_8})
_mergecubes2_43 = oeop.merge_cubes(**{'cube2': _apply3_8, 'cube1': _reducedimension1_26, 'overlap_resolver': _gt2_44})
_saveresult2_45 = oeop.save_result(**{'data': _mergecubes2_43, 'format': 'NetCDF', 'options': {}})
