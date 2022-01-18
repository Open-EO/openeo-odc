"""

"""

from openeo_odc.map_processes_odc import map_general, map_load_collection, map_load_result
from openeo_odc.utils import ExtraFuncUtils, PROCS_WITH_VARS


def map_to_odc(graph, odc_env, odc_url):
    """Map openEO process graph to xarray/opendatacube functions."""
    extra_func_utils = ExtraFuncUtils()

    nodes = {}
    extra_func = {}
    for k, node_id in enumerate(graph.ids):
        cur_node = graph[node_id]
        parent_proc_id = cur_node.parent_process.process_id if cur_node.parent_process else None

        kwargs = {}
        kwargs['from_parameter'] = resolve_from_parameter(cur_node)
        if len(cur_node.result_processes) == 1:
            kwargs['result_node'] = cur_node.result_processes[0].id
        if cur_node.parent_process: #parent process can be eiter reduce_dimension or apply
            if parent_proc_id == 'reduce_dimension':
                # in apply and reduce_dimension the process is the node of the child process
                kwargs['dimension'] = cur_node.parent_process.content['arguments']['dimension']
            if parent_proc_id == 'apply_dimension':
                # in apply_dimension the process should be a callable process, for example 'mean'.
                # the 'mean' gets transformed to 'oeop.mean' in the string_creation.create_param_string function.
                kwargs['dimension'] = cur_node.parent_process.content['arguments']['dimension']
                cur_node.parent_process.content['arguments']['process'] = cur_node.process_id
            if parent_proc_id == 'aggregate_temporal_period':
                cur_node.parent_process.content['arguments']['reducer'] = cur_node.process_id
            if parent_proc_id == 'filter_labels':
                cur_node.parent_process.content['arguments']['condition'] = cur_node.process_id

        if cur_node.process_id in PROCS_WITH_VARS:
            cur_node.content['arguments']['function'] = extra_func_utils.get_func_name(cur_node.id)
            extra_func[extra_func_utils.get_dict_key(cur_node.id)][f"return_{cur_node.id}"] = f"    return _{kwargs.pop('result_node')}\n\n"

        param_sets = [{'x', 'y'}, {'x', }, {'data', 'value'}, {'base', 'p'}, {'data', }]
        if cur_node.process_id == 'load_collection':
            cur_node_content = map_load_collection(cur_node.id, cur_node.content)
        elif cur_node.process_id == 'load_result':
            cur_node_content = map_load_result(cur_node.id, cur_node.content)
        elif (params in set(cur_node.arguments.keys()) for params in param_sets):
            if cur_node.parent_process and parent_proc_id in PROCS_WITH_VARS:
                cur_node_content = map_general(cur_node.id, cur_node.content, kwargs,
                                               donot_map_params=PROCS_WITH_VARS[parent_proc_id].list)
            else:
                cur_node_content = map_general(cur_node.id, cur_node.content, kwargs)
        else:
            raise ValueError(f"Node {cur_node.id} with arguments {cur_node.arguments.keys()} could not be mapped!")

        # Handle fit_curve / predict_curve sub-process-graph
        if cur_node.parent_process and parent_proc_id in PROCS_WITH_VARS:
            fc_id = cur_node.parent_process.id
            fc_name = extra_func_utils.get_dict_key(fc_id)
            if fc_name not in extra_func:
                extra_func[fc_name] = {
                    f"func_header_{fc_id}": extra_func_utils.get_func_header(fc_id, PROCS_WITH_VARS[parent_proc_id].str)
                }
            extra_func[fc_name][cur_node.id] = f"    {cur_node_content}"  # add indentation
        else:
            nodes[cur_node.id] = cur_node_content

    final_fc = {}
    for fc_proc in extra_func.values():
        final_fc.update(**fc_proc)
    return {
        'header': create_job_header(odc_env_collection=odc_env, dask_url=odc_url),
        **final_fc,
        **nodes,
    }


def resolve_from_parameter(node):
    """ Resolve 'from_parameter' dependencies.

    Converts e.g. {'from_parameter': 'data'} to {'data': 'dc_0'}

    """
    overlap_resolver_map = {"x": "cube1", "y": "cube2"}

    in_nodes = {}

    # Resolve 'from_parameter' if field exists in node arguments
    for argument in node.arguments:
        # Check if current argument is iterable, else skip to next one
        try:
            _ = iter(node.arguments[argument])
        except TypeError:
            # Argument is not iterable (e.g. 1 or None)
            continue
        if 'from_parameter' in node.arguments[argument]:
            try:
                from_param_name = node.arguments[argument]['from_parameter']
                # Handle overlap resolver for merge_cubes process
                if node.parent_process.process_id == "merge_cubes" and \
                        "overlap_resolver" in node.parent_process.arguments and \
                        node.parent_process.arguments["overlap_resolver"]["from_node"] == node.id:
                    parent_data_key = overlap_resolver_map[from_param_name]
                    in_nodes[from_param_name] = node.parent_process.arguments[parent_data_key]["from_node"]
                else:
                    # expected that parent process holds parameter in "data" argument
                    in_nodes[from_param_name] = node.parent_process.arguments['data']['from_node']
            except KeyError:
                pass

    return in_nodes


def create_job_header(dask_url: str, odc_env_collection: str = "default", odc_env_user_gen: str = "user_generated"):
    """Create job imports."""
    return f"""from dask.distributed import Client
import datacube
import openeo_processes as oeop

# Initialize ODC instance
cube = datacube.Datacube(app='collection', env='{odc_env_collection}')
cube_user_gen = datacube.Datacube(app='user_gen', env='{odc_env_user_gen}')
# Connect to Dask Scheduler
client = Client('{dask_url}')
"""
