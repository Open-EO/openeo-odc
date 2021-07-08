"""

"""

from openeo_odc.map_processes_odc import map_required, map_data, map_load_collection


def map_to_odc(graph, odc_env, odc_url):
    """Map openEO process graph to xarray/opendatacube functions."""

    nodes = {
        'header': create_job_header(odc_env, odc_url)
    }
    for k, node_id in enumerate(graph.ids):
        cur_node = graph[node_id]

        kwargs = {}
        kwargs['from_parameter'] = resolve_from_parameter(cur_node)
        if len(cur_node.result_processes) == 1:
            kwargs['result_node'] = cur_node.result_processes[0].id
        if cur_node.parent_process: #parent process can be eiter reduce_dimension or apply
            if cur_node.parent_process.process_id == 'reduce_dimension':
                kwargs['dimension'] = cur_node.parent_process.content['arguments']['dimension']
        if tuple(cur_node.arguments.keys()) in [('x', 'y'), ('x',), ('data', 'value'), ('base', 'p')]:
            nodes[cur_node.id] = map_required(cur_node.id, cur_node.content)
        elif 'data' in tuple(cur_node.arguments.keys()):
            nodes[cur_node.id] = map_data(cur_node.id, cur_node.content, kwargs)
        elif 'id' in tuple(cur_node.arguments.keys()):
            # This should be only load_collection
            nodes[cur_node.id] = map_load_collection(cur_node.id, cur_node.content)
        else:
            raise ValueError(f"Node {cur_node.id} with arguments {cur_node.arguments.keys()} could not be mapped!")

    return nodes


def resolve_from_parameter(node):
    """ Resolve 'from_parameter' dependencies.

    Converts e.g. {'from_parameter': 'data'} to {'data': 'dc_0'}

    """

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
            in_nodes[argument] = node.parent_process.arguments['data']['from_node']

    return in_nodes


def create_job_header(odc_env: str, dask_url: str):
    """Create job imports."""
    if odc_env is None:
        return f"""from dask.distributed import Client
import datacube
import openeo_processes as oeop

# Initialize ODC instance
cube = datacube.Datacube()
# Connect to Dask Scheduler
client = Client('{dask_url}')
"""
    else:
        return f"""from dask.distributed import Client
import datacube
import openeo_processes as oeop

# Initialize ODC instance
cube = datacube.Datacube(app='app_1', env='{odc_env}')
# Connect to Dask Scheduler
client = Client('{dask_url}')
"""
