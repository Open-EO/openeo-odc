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


def extra_func_4_4(x, *parameters):
    _multiply1_5 = oeop.multiply(**{'x': 1.991021277657232e-07, 'y': x})
    _multiply3_7 = oeop.multiply(**{'x': 1.991021277657232e-07, 'y': x})
    _arrayelement3_9 = oeop.array_element(**{'data': parameters, 'index': 2})
    _arrayelement2_10 = oeop.array_element(**{'data': parameters, 'index': 1})
    _arrayelement1_11 = oeop.array_element(**{'data': parameters, 'index': 0})
    _cos1_8 = oeop.cos(**{'x': _multiply1_5})
    _sin1_12 = oeop.sin(**{'x': _multiply3_7})
    _multiply2_6 = oeop.multiply(**{'x': _arrayelement2_10, 'y': _cos1_8})
    _multiply4_13 = oeop.multiply(**{'x': _arrayelement3_9, 'y': _sin1_12})
    _add1_15 = oeop.add(**{'x': _arrayelement1_11, 'y': _multiply2_6})
    _add2_14 = oeop.add(**{'x': _add1_15, 'y': _multiply4_13})
    return _add2_14



def extra_func_fitcurve1_22(x, *parameters):
    _arrayelement1_25 = oeop.array_element(**{'data': parameters, 'index': 0})
    _arrayelement2_26 = oeop.array_element(**{'data': parameters, 'index': 1})
    _arrayelement3_27 = oeop.array_element(**{'data': parameters, 'index': 2})
    _multiply1_29 = oeop.multiply(**{'x': 1.991021277657232e-07, 'y': x})
    _multiply3_31 = oeop.multiply(**{'x': 1.991021277657232e-07, 'y': x})
    _cos1_28 = oeop.cos(**{'x': _multiply1_29})
    _sin1_33 = oeop.sin(**{'x': _multiply3_31})
    _multiply2_30 = oeop.multiply(**{'x': _arrayelement2_26, 'y': _cos1_28})
    _multiply4_32 = oeop.multiply(**{'x': _arrayelement3_27, 'y': _sin1_33})
    _add1_23 = oeop.add(**{'x': _arrayelement1_25, 'y': _multiply2_30})
    _add2_24 = oeop.add(**{'x': _add1_23, 'y': _multiply4_32})
    return _add2_24


_loadcollection1_16 = oeop.load_collection(odc_cube=cube, **{'product': 'boa_sentinel_2', 'dask_chunks': {'y': 12160, 'x': 12114, 'time': 'auto'}, 'x': (11.410299, 11.413905), 'y': (46.341515, 46.343144), 'time': ['2016-09-01', '2018-08-31'], 'measurements': ['B02', 'B03', 'B04', 'B05', 'B08']})
_1_1 = oeop.eq(**{'x': _loadcollection1_16, 'y': 0})
_clip1_18 = oeop.clip(**{'x': _loadcollection1_16, 'max': 4000, 'min': 0})
_clip1_20 = oeop.clip(**{'x': _loadcollection1_16, 'min': 0, 'max': 4000})
_2_0 = oeop.apply(**{'data': _1_1, 'process': _1_1})
_apply2_17 = oeop.apply(**{'data': _clip1_18, 'process': _clip1_18})
_apply3_19 = oeop.apply(**{'process': _clip1_20, 'data': _clip1_20})
_dimension_labels1_21 = oeop.dimension_labels(**{'data': _apply2_17, 'dimension': 't'})
_4_4 = oeop.fit_curve(**{'parameters': [1, 1, 1], 'data': _apply3_19, 'function': extra_func_4_4, 'dimension': 't'})
_fitcurve1_22 = oeop.predict_curve(**{'data': _apply2_17, 'function': extra_func_fitcurve1_22, 'parameters': _4_4, 'labels': _dimension_labels1_21, 'dimension': 't'})
_1_3 = oeop.multiply(**{'x': _fitcurve1_22, 'y': _2_0})
_3_2 = oeop.merge_cubes(**{'cube1': _fitcurve1_22, 'cube2': _2_0, 'overlap_resolver': _1_3})
_saveresult1_34 = oeop.save_result(**{'data': _3_2, 'format': 'NetCDF', 'options': {}})
cluster.shutdown()
gateway.close()