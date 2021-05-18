"""

"""


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
        'x': (process['arguments']['spatial_extent']['west'],
              process['arguments']['spatial_extent']['east']),
        'y': (process['arguments']['spatial_extent']['south'],
              process['arguments']['spatial_extent']['north']),
        'time': process['arguments']['temporal_extent'],
        'dask_chunks': {'time': 'auto', 'x': 1000, 'y': 1000},
        }
    if 'crs' in process['arguments']['spatial_extent']:
        params['crs'] = process['arguments']['spatial_extent']['crs']
    if 'bands' in process['arguments']:
        params['measurements'] = process['arguments']['bands']

    return f"""
{id} = oeop.load_collection(odc_cube=cube, **{params})
"""


def map_xy(id, process):
    """Map to xarray version of processes with input (x, y).

    Creates a string like the following:
    mul_6 = oeop.subtract(**{'x': dep_1, 'y': dep_2})

    Returns: str

    """

    process_name = process['process_id']
    params = {
        'x': process['arguments']['x'],
        'y': process['arguments']['y']
    }
    if isinstance(params['x'], dict) and 'from_node' in params['x']:
        params['x'] = params['x']['from_node']
    if isinstance(params['y'], dict) and 'from_node' in params['y']:
        params['y'] = params['y']['from_node']
    params = convert_from_node_parameter(params)
    params_str = create_string(params)

    return f"""{id} = oeop.{process_name}({params_str})
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
        params['data'] = kwargs['result_node']
        params['reducer'] = {}
    else:
        params['data'] = convert_from_node_parameter(params['data'],
                                                     kwargs['from_parameter'])
    if 'dimension' in kwargs and not isinstance(params['data'], list):
        kwargs['dimension'] = check_dimension(kwargs['dimension'])
    elif 'dimension' in kwargs:
        # Do not map 'dimension' for processes like `sum`
        _ = kwargs.pop('dimension', None)
    _ = kwargs.pop('from_parameter', None)
    _ = kwargs.pop('result_node', None)
    params = {**params, **kwargs}

    params_str = create_string(params)

    return f"""{id} = oeop.{process_name}({params_str})
"""


def convert_from_node_parameter(args_in, from_par=None):
    """ Convert from_node and resolve from_parameter dependencies."""

    if not isinstance(args_in, list):
        args_in = [args_in]

    for k, item in enumerate(args_in):
        if isinstance(item, dict) and 'from_node' in item:
            args_in[k] = item['from_node']
        if from_par and isinstance(item, dict) and 'from_parameter' in item:
            args_in[k] = from_par[item['from_parameter']]

    if len(args_in) == 1:
        args_in = args_in[0]

    return args_in


def check_dimension(in_value):
    """ """

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
        if key in ('x', 'y', 'data'):
            to_remove.append(key)
            if isinstance(value, list):
                val_str = "["
                for val in value:
                    val_str += str(val) + ', '
                inputs.append(f"'{key}':{val_str[:-2]}]")
            else:
                inputs.append(f"'{key}':{value}")
        else:
            continue

    for key in to_remove:
        _ = dict_input.pop(key)

    replace_str = '{' + ','.join(inputs)
    if dict_input:
        replace_str += ', '

    return f"**{dict_input}".replace('{', replace_str, 1)
