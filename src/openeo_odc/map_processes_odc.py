"""

"""
from datetime import datetime
import numpy as np
from copy import deepcopy


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
        'dask_chunks': {'time': 'auto', 'x': 1000, 'y': 1000},
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
    if 'bands' in process['arguments'] and len(process['arguments']['bands'])>0:
        params['measurements'] = process['arguments']['bands']

    return f"""
{'_'+id} = oeop.load_collection(odc_cube=cube, **{params})
"""


def map_required(id, process, kwargs=None) -> str:
    """Map processes with required arguments only.

    Currently processes with params ('x', 'y'), ('data', 'value'), ('base', 'p'), and ('x') are supported.
    Creates a string like the following: sub_6 = oeop.subtract(**{'x': dep_1, 'y': dep_2})

    Returns: str
    """
    process_name = process['process_id']
    params = deepcopy(process['arguments'])
    from_param = kwargs['from_parameter'] if kwargs and 'from_parameter' in kwargs else None
    for key in params:
        params[key] = convert_from_node_parameter(params[key], from_param)
    params_str = create_string(params)

    return f"""{'_'+id} = oeop.{process_name}({params_str})
"""

def map_data(id, process, kwargs):
    """Map to xarray version of processes with input (data, param_1, ?param2, ...).

    Creates a string like the following:
    sum_node = oeop.sum([nir, p1, p2], extra_values=[1])

    Returns: str

    """

    process_name = process['process_id']
    params = process['arguments']
    if 'result_node' in kwargs:
        params['data'] = '_' + kwargs['result_node']
        if process_name != 'apply':
            params['reducer'] = {}
    else:
        params['data'] = convert_from_node_parameter(params['data'],
                                                     kwargs['from_parameter'])
    if 'target' in params: #target is used in resample_cube_spatial for example
        params['target'] = convert_from_node_parameter(params['target'],
                                                     kwargs['from_parameter'])
    if 'dimension' in kwargs and not isinstance(params['data'], list):
        kwargs['dimension'] = check_dimension(kwargs['dimension'])
    elif 'dimension' in kwargs:
        # Do not map 'dimension' for processes like `sum` and `apply`
        _ = kwargs.pop('dimension', None)
    _ = kwargs.pop('from_parameter', None)
    _ = kwargs.pop('result_node', None)
    params = {**params, **kwargs}

    params_str = create_string(params)
    return f"""{'_'+id} = oeop.{process_name}({params_str})
"""


def convert_from_node_parameter(args_in, from_par=None):
    """ Convert from_node and resolve from_parameter dependencies."""

    if not isinstance(args_in, list):
        args_in = [args_in]

    for k, item in enumerate(args_in):
        if isinstance(item, dict) and 'from_node' in item:
            args_in[k] = '_' + item['from_node']
        if from_par \
                and isinstance(item, dict) \
                and 'from_parameter' in item \
                and item['from_parameter'] in from_par.keys():
            args_in[k] = '_' + from_par[item['from_parameter']]

    if len(args_in) == 1:
        args_in = args_in[0]

    return args_in


def check_dimension(in_value):
    """ Convert common dimension names to a preset value."""

    if in_value in ('t', 'time', 'temporal'):
        out_value = 'time'
    elif in_value in ('s', 'band', 'bands', 'spectral'):
        out_value = 'bands'
    else:
        out_value = in_value

    return out_value


def create_string(dict_input):
    """Creates a string where 'x', 'y' and 'data' fields are not mapped to str.

    For example:
    **{'data':dc_0,'index':0, 'dimension':'bands'}
    **{'data':[10000, nir_2, p1_6, p2_7]}
    **{'x':nir_2,'y':red_3}

    """

    inputs = []
    to_remove = []
    for key, value in dict_input.items():
        if key in ('x', 'y', 'data', 'value', 'base', 'p','target'):
            to_remove.append(key)
            if isinstance(value, list):
                val_str = "["
                for val in value:
                    val_str += str(val) + ', '
                inputs.append(f"'{key}': {val_str[:-2]}]")
            else:
                inputs.append(f"'{key}': {value}")
        else:
            continue

    for key in to_remove:
        _ = dict_input.pop(key)

    replace_str = '{' + ','.join(inputs)
    if dict_input:
        replace_str += ', '

    return f"**{dict_input}".replace('{', replace_str, 1)
