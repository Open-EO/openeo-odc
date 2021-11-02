# key: process name (default is used for all process not explicitly listed)
# value: parameter keys where ' should be removed from the values > so turned from string into python variable
PROCESS_ARG_MAP = {
    'default': ['x', 'y', 'data', 'value', 'base', 'p', 'target', 'parameters', 'function', 'process', 'cube1',
                   'cube2', 'overlap_resolver', 'labels', 'mask'],
    'rename_labels': ['data'],
}


def create_param_string(dict_input: dict, process_name: str):
    """Creates a parameter string, converting the defined keys from string to python variable.

    For example:
    **{'data':dc_0,'index':0, 'dimension':'bands'}
    **{'data':[10000, nir_2, p1_6, p2_7]}
    **{'x':nir_2,'y':red_3}

    """
    inputs = []
    to_remove = []
    keys_to_extract = PROCESS_ARG_MAP[process_name] if process_name in PROCESS_ARG_MAP else PROCESS_ARG_MAP["default"]
    for key, value in dict_input.items():
        if key in keys_to_extract:
            to_remove.append(key)
            # label can hold node references and datetime stings > this extra handling is required
            if key == 'labels':
                if isinstance(value, str) or value is None:
                   if value is None or value.startswith('_'):
                       inputs.append(f"'{key}': {value}")
                   else:
                       inputs.append(f"'{key}': '{value}'")
                elif isinstance(value, list):
                    val_str = "["
                    for val in value:
                        if (not isinstance(val, str)) or (isinstance(val, str) and val.startswith('_')):
                            val_str += str(val) + ', '
                        else:
                            val_str += f"'{val}', "
                    inputs.append(f"'{key}': {val_str[:-2]}]")
            elif isinstance(value, list):
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

    replace_str = '{' + ', '.join(inputs)
    if dict_input:
        replace_str += ', '

    return f"**{dict_input}".replace('{', replace_str, 1)
