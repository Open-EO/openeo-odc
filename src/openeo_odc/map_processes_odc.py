"""

"""
from datetime import datetime
import numpy as np
from copy import deepcopy
from typing import List

from openeo_odc.utils import get_oeop_str
from openeo_odc.string_creation import create_param_string


def map_load_collection(id, process):
    """ Map to load_collection process for ODC datacubes.

    Creates a string like the following:
    dc = oeop.load_collection(odc_cube=datacube,
                              **{'product': 'B_Sentinel_2',
                                 'x': (11.28, 11.41),
                                 'y': (46.52, 46.46),
                                 'time': ['2018-06-04', '2018-06-23'],
                                 'dask_chunks': {'time': 'auto',
                                                 'x': 'auto',
                                                 'y': 'auto'},
                                 'measurements': ['B08', 'B04', 'B02']})

    Returns: str

    """

    params = {
        'product': process['arguments']['id'],
        'dask_chunks': {'time': 'auto', 'x': 10000, 'y': 10000},
        }
    if 'spatial_extent' in process['arguments']:
        if process['arguments']['spatial_extent'] is not None:
            if 'south' in process['arguments']['spatial_extent'] and \
               'north' in process['arguments']['spatial_extent'] and \
               'east'  in process['arguments']['spatial_extent'] and \
               'west'  in process['arguments']['spatial_extent']:
                params['x'] = (process['arguments']['spatial_extent']['west'],process['arguments']['spatial_extent']['east'])
                params['y'] = (process['arguments']['spatial_extent']['south'],process['arguments']['spatial_extent']['north'])
            elif 'coordinates' in process['arguments']['spatial_extent']:
                # Pass coordinates to odc and process them there
                # TODO: data has to be masked after loading with a polygon
                polygon = process['arguments']['spatial_extent']['coordinates']
                if polygon is not None:
                    lowLat      = np.min([[el[1] for el in polygon[0]]])
                    highLat     = np.max([[el[1] for el in polygon[0]]])
                    lowLon      = np.min([[el[0] for el in polygon[0]]])
                    highLon     = np.max([[el[0] for el in polygon[0]]])
                    params['x'] = (lowLon,highLon)
                    params['y'] = (lowLat,highLat)
    params['time'] = []
    if 'temporal_extent' in process['arguments']:
        def exclusive_date(date):
            return np.datetime_as_string(np.datetime64(date) - np.timedelta64(1, 'D'), timezone='UTC') # Substracts one day
        if process['arguments']['temporal_extent'] is not None and len(process['arguments']['temporal_extent'])>0:
            timeStart = '1970-01-01'
            timeEnd   = str(datetime.now()).split(' ')[0] # Today is the default date for timeEnd, to include all the dates if not specified
            if process['arguments']['temporal_extent'][0] is not None:
                timeStart = process['arguments']['temporal_extent'][0]
            if process['arguments']['temporal_extent'][1] is not None:
                timeEnd = process['arguments']['temporal_extent'][1]
            params['time'] = [timeStart,exclusive_date(timeEnd)] 
    if 'crs' in process['arguments']['spatial_extent']:
        params['crs'] = process['arguments']['spatial_extent']['crs']
    params['measurements'] = []
    if 'bands' in process['arguments'] and process['arguments']['bands'] is not None and len(process['arguments']['bands'])>0:
        params['measurements'] = process['arguments']['bands']

    return f"""
{'_'+id} = oeop.load_collection(odc_cube=cube, **{params})
"""

def map_load_result(id, process) -> str:
    """Map load_result process.

    This needs to be handled separately because the user_generated ODC environment / cube must be used.
    """
    # ODC does not allow "-" in product names, there the job_id is slightly adopted to retrieve the product name
    product_name = process['arguments']['id'].replace("-", "_")
    params = {
        'product': product_name,
        'dask_chunks': {'time': 'auto', 'x': 10000, 'y': 10000},
        }
    if 'spatial_extent' in process['arguments']:
        if process['arguments']['spatial_extent'] is not None:
            if 'south' in process['arguments']['spatial_extent'] and \
               'north' in process['arguments']['spatial_extent'] and \
               'east'  in process['arguments']['spatial_extent'] and \
               'west'  in process['arguments']['spatial_extent']:
                params['x'] = (process['arguments']['spatial_extent']['west'],process['arguments']['spatial_extent']['east'])
                params['y'] = (process['arguments']['spatial_extent']['south'],process['arguments']['spatial_extent']['north'])
            elif 'coordinates' in process['arguments']['spatial_extent']:
                # Pass coordinates to odc and process them there
                # TODO: data has to be masked after loading with a polygon
                polygon = process['arguments']['spatial_extent']['coordinates']
                if polygon is not None:
                    lowLat      = np.min([[el[1] for el in polygon[0]]])
                    highLat     = np.max([[el[1] for el in polygon[0]]])
                    lowLon      = np.min([[el[0] for el in polygon[0]]])
                    highLon     = np.max([[el[0] for el in polygon[0]]])
                    params['x'] = (lowLon,highLon)
                    params['y'] = (lowLat,highLat)
            if 'crs' in process['arguments']['spatial_extent']:
                params['crs'] = process['arguments']['spatial_extent']['crs']

    if 'temporal_extent' in process['arguments']:
        params['time'] = []
        def exclusive_date(date):
            return np.datetime_as_string(np.datetime64(date) - np.timedelta64(1, 'D'), timezone='UTC') # Substracts one day
        if process['arguments']['temporal_extent'] is not None and len(process['arguments']['temporal_extent'])>0:
            timeStart = '1970-01-01'
            timeEnd   = str(datetime.now()).split(' ')[0] # Today is the default date for timeEnd, to include all the dates if not specified
            if process['arguments']['temporal_extent'][0] is not None:
                timeStart = process['arguments']['temporal_extent'][0]
            if process['arguments']['temporal_extent'][1] is not None:
                timeEnd = process['arguments']['temporal_extent'][1]
            params['time'] = [timeStart,exclusive_date(timeEnd)]

    if 'bands' in process['arguments'] and process['arguments']['bands'] is not None and len(process['arguments']['bands'])>0:
        params['measurements'] = []
        params['measurements'] = process['arguments']['bands']

    return f"""
_{id} = oeop.load_result(odc_cube=cube_user_gen, **{params})
"""


def map_general(id, process, kwargs=None, donot_map_params: List[str] = None) -> str:
    """Map processes with required arguments only.

    Currently processes with params ('x', 'y'), ('data', 'value'), ('base', 'p'), and ('x') are supported.
    Creates a string like the following: sub_6 = oeop.subtract(**{'x': dep_1, 'y': dep_2})

    Returns: str
    """
    if kwargs is None:
        kwargs = {}
    process_name = process['process_id']
    params = deepcopy(process['arguments'])
    from_param = kwargs['from_parameter'] if kwargs and 'from_parameter' in kwargs else None
    if 'result_node' in kwargs: #if result_node is in kwargs, data must always be in params
        if process_name not in ['merge_cubes', 'apply_dimension', 'aggregate_temporal_period', 'filter_labels']:
            params['data'] = '_' + kwargs['result_node']
        if process_name not in ['apply', 'fit_curve', 'predict_curve', 'merge_cubes', 'apply_dimension', 'aggregate_temporal_period', 'filter_labels']:
            params['reducer'] = {}
        _ = kwargs.pop('result_node', None)
    for key in params:
        params[key] = convert_from_node_parameter(params[key], from_param, donot_map_params)

    if 'data' in params:
        if 'dimension' in kwargs and not isinstance(params['data'], list):
            kwargs['dimension'] = check_dimension(kwargs['dimension'])
        elif 'dimension' in kwargs:
            # Do not map 'dimension' for processes like `sum` and `apply`
            _ = kwargs.pop('dimension', None)
        _ = kwargs.pop('from_parameter', None)
        params = {**params, **kwargs}

    params_str = create_param_string(params, process_name)
    return get_oeop_str(id, process_name, params_str)


def convert_from_node_parameter(args_in, from_par=None, donot_map_params: List[str] = None):
    """ Convert from_node and resolve from_parameter dependencies."""

    was_list = True
    if not isinstance(args_in, list):
        args_in = [args_in]
        was_list = False

    for k, item in enumerate(args_in):
        if isinstance(item, dict) and 'from_node' in item:
            args_in[k] = '_' + item['from_node']
        if from_par \
                and isinstance(item, dict) \
                and 'from_parameter' in item \
                and item['from_parameter'] in from_par.keys():
            if donot_map_params and item['from_parameter'] in donot_map_params:
                args_in[k] = item["from_parameter"]
            elif isinstance(from_par[item['from_parameter']], str):
                args_in[k] = '_' + from_par[item['from_parameter']]
            else:
                args_in[k] = from_par[item['from_parameter']]
        elif isinstance(item, dict) and 'from_parameter' in item:
            args_in[k] = item["from_parameter"]

    if len(args_in) == 1 and not was_list:
        args_in = args_in[0]

    return args_in


def check_dimension(in_value):
    """ Convert common dimension names to a preset value."""

    if in_value in ('t', 'time', 'temporal'):
        return 'time'
    elif in_value in ('s', 'band', 'bands', 'spectral'):
        return 'bands'
    return in_value
