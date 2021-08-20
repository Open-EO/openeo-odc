"""
"""

import json
import os

import pytest
from openeo_odc.map_to_odc import map_to_odc
from openeo_pg_parser.translate import translate_process_graph
from openeo_pg_parser.validate import validate_processes


@pytest.mark.parametrize(
    "job_name", ("evi", "apply_job", "fit_curve", "load_result", "fit_curve_cm", "uc6_3")
)
def test_job(job_name: str):
    """Create a xarray/opendatacube job based on an openEO process graph."""

    # Set input parameters
    tests_folder = os.path.dirname(os.path.abspath(__file__))
    process_graph_json = os.path.join(tests_folder, f"process_graphs/{job_name}.json")
    process_defs = json.load(open(
        os.path.join(tests_folder, 'backend_processes.json')
        ))['processes']
    odc_env = 'default'
    odc_url = 'tcp://xx.yyy.zz.kk:8786'

    graph = translate_process_graph(process_graph_json,
                                    process_defs).sort(by='result')
    # Check if process graph is valid
    validate_processes(graph, process_defs)
    nodes = map_to_odc(graph, odc_env, odc_url)

    # Write to disk
    job_file = f"{job_name}_odc"
    job_py = f"{job_file}.py"
    with open(job_py, "w") as f:
        for node in nodes:
            f.write(nodes[node])

    # Check it matches the reference file
    with open(job_py) as f:
        this_file = f.readlines()

    with open(os.path.join(tests_folder, f"ref_jobs/{job_file}_ref.py")) as f:
        ref_file = f.readlines()
    assert this_file == ref_file
    # Clean up
    os.remove(job_py)

