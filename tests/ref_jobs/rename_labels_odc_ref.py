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


def extra_func_fitcurve1_4(x, *parameters):
    _arrayelement4_5 = oeop.array_element(**{'data': parameters, 'index': 0})
    _arrayelement5_6 = oeop.array_element(**{'data': parameters, 'index': 1})
    _multiply4_7 = oeop.multiply(**{'x': 1.991021277657232e-07, 'y': x})
    _arrayelement6_11 = oeop.array_element(**{'data': parameters, 'index': 2})
    _cos2_8 = oeop.cos(**{'x': _multiply4_7})
    _sin2_12 = oeop.sin(**{'x': _multiply4_7})
    _multiply5_9 = oeop.multiply(**{'x': _arrayelement5_6, 'y': _cos2_8})
    _multiply6_13 = oeop.multiply(**{'x': _arrayelement6_11, 'y': _sin2_12})
    _add3_10 = oeop.add(**{'x': _arrayelement4_5, 'y': _multiply5_9})
    _add4_14 = oeop.add(**{'x': _add3_10, 'y': _multiply6_13})
    return _add4_14



def extra_func_predictcurve1_15(x, *parameters):
    _arrayelement1_16 = oeop.array_element(**{'data': parameters, 'index': 0})
    _arrayelement2_17 = oeop.array_element(**{'data': parameters, 'index': 1})
    _multiply1_18 = oeop.multiply(**{'x': 1.991021277657232e-07, 'y': x})
    _arrayelement3_22 = oeop.array_element(**{'data': parameters, 'index': 2})
    _cos1_19 = oeop.cos(**{'x': _multiply1_18})
    _sin1_23 = oeop.sin(**{'x': _multiply1_18})
    _multiply2_20 = oeop.multiply(**{'x': _arrayelement2_17, 'y': _cos1_19})
    _multiply3_24 = oeop.multiply(**{'x': _arrayelement3_22, 'y': _sin1_23})
    _add1_21 = oeop.add(**{'x': _arrayelement1_16, 'y': _multiply2_20})
    _add2_25 = oeop.add(**{'x': _add1_21, 'y': _multiply3_24})
    return _add2_25


_loadcollection1_0 = oeop.load_collection(odc_cube=cube, **{'product': 'boa_sentinel_2', 'dask_chunks': {'y': 12160, 'x': 12114, 'time': 'auto'}, 'x': (11.411777, 11.411977), 'y': (46.342355, 46.342555000000004), 'time': ['2016-09-01', '2019-08-31'], 'measurements': ['B08']})
_clip1_2 = oeop.clip(**{'x': _loadcollection1_0, 'max': 5000, 'min': 0})
_apply1_1 = oeop.apply(**{'data': _clip1_2, 'process': _clip1_2})
_dimensionlabels1_3 = oeop.dimension_labels(**{'data': _apply1_1, 'dimension': 't'})
_fitcurve1_4 = oeop.fit_curve(**{'data': _apply1_1, 'function': extra_func_fitcurve1_4, 'parameters': [1, 1, 1], 'dimension': 't'})
_predictcurve1_15 = oeop.predict_curve(**{'data': _apply1_1, 'function': extra_func_predictcurve1_15, 'labels': _dimensionlabels1_3, 'parameters': _fitcurve1_4, 'dimension': 't'})
_renamelabels1_26 = oeop.rename_labels(**{'data': _predictcurve1_15, 'dimension': 'bands', 'target': ['B08_predicted']})
_mergecubes1_27 = oeop.merge_cubes(**{'cube1': _apply1_1, 'cube2': _renamelabels1_26})
_saveresult1_28 = oeop.save_result(**{'data': _mergecubes1_27, 'format': 'NetCDF', 'options': {}})
cluster.shutdown()
gateway.close()