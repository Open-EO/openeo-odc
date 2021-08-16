"""
Tests processes with input type func(x, y)
"""
from typing import Tuple, Any, Dict

import pytest
from openeo_odc.map_processes_odc import map_general

from tests.utils import create_params, set_process


@pytest.fixture(
    params=[
        ({'x': 3, 'y': 6}, "{'x': 3, 'y': 6}"),
        ({'x': {'from_node': 'red_3'}, 'y': 6}, "{'x': _red_3, 'y': 6}"),
        ({'x': {'from_node': 'red_3'}, 'y': {'from_node': 'blue_2'}}, "{'x': _red_3, 'y': _blue_2}"),
    ]
)
def get_xy_param(request):
    return create_params("node_xy", request.param[0], request.param[1])


@pytest.fixture(
    params=[
        ({'x': 3}, "{'x': 3}"),
        ({'x': {'from_node': 'red_3'}}, "{'x': _red_3}"),
    ]
)
def get_x_param(request):
    return create_params("node_x", request.param[0], request.param[1])


@pytest.fixture(
    params=[
        ({'data': [1, 2, 3, 4], 'value': 4}, "{'data': [1, 2, 3, 4], 'value': 4}"),
        # ({'data': ['1', '2', '3', '4'], 'value': '4'}, "{'data': ['1', '2', '3', '4'],'value': '4'}"),
        # ({'data': ['this', 'is', 'a', 'test'], 'value': 'test'}, "{'data': ['this', 'is', 'a', 'test'],'value': 'test'}"),
        ({'data': {'from_node': 'red_3'}, 'value': 4}, "{'data': _red_3, 'value': 4}"),
        ({'data': [1, 2, 3, 4], 'value': {'from_node': 'blue_2'}}, "{'data': [1, 2, 3, 4], 'value': _blue_2}"),
    ]
)
def get_data_val_param(request):
    return create_params("node_data_val", request.param[0], request.param[1])


@pytest.mark.parametrize("process", (
        "gt",
        "gte",
        "lt",
        "lte",
        "add",
        "divide",
        "mod",
        "subtract",
        "multiply",
        "normalized_difference",
        "arctan2",
        "and_",
        "or_",
        "xor",
))
def test_xy(process: str, get_xy_param: Tuple[str, Dict[str, Any], str]):
    """Test conversions of processes with input x, y."""
    node_id, process_def, process_ref = set_process(*get_xy_param, process)

    out = map_general(node_id, process_def)

    assert out == process_ref


@pytest.mark.parametrize("process", (
        "is_nan",
        "is_no_data",
        "is_valid",
        "not",
        "absolute",
        "int",
        "sgn",
        "sqrt",
        "constant",
        "ln",
        "ceil",
        "floor",
        "arccos",
        "arcsin",
        "arctan",
        "arcsinh",
        "arctanh",
        "cos",
        "cosh",
        "sin",
        "sinh",
        "tan",
        "tanh",
))
def test_x(process: str, get_x_param: Tuple[str, Dict[str, Any], str]):
    """Test conversions of processes with input x."""
    node_id, process_def, process_ref = set_process(*get_x_param, process)

    out = map_general(node_id, process_def)

    assert out == process_ref


@pytest.mark.parametrize('process', (
        "array_contains",
        "array_find",
))
def test_data_value(process: str, get_data_val_param: Tuple[str, Dict[str, Any], str]):
    node_id, process_def, process_ref = set_process(*get_data_val_param, process)

    out = map_general(node_id, process_def)

    assert out == process_ref


@pytest.mark.parametrize("arg_in,arg_out", (
        ({'base': 4, 'p': 2}, "{'base': 4, 'p': 2}"),
        ({'base': 4.1, 'p': 2.9}, "{'base': 4.1, 'p': 2.9}"),
        ({'base': {'from_node': 'red_3'}, 'p': 4}, "{'base': _red_3, 'p': 4}"),
        ({'base': 4, 'p': {'from_node': 'blue_2'}}, "{'base': 4, 'p': _blue_2}"),
        ({'base': {'from_node': 'red_3'}, 'p': {'from_node': 'blue_2'}}, "{'base': _red_3, 'p': _blue_2}"),
))
def test_power(arg_in: Dict[str, Any], arg_out: str):
    """Test conversions of power process with input base, p."""
    node_id, process_def, process_ref = set_process(*create_params("node_power", arg_in, arg_out), "power")

    out = map_general(node_id, process_def)

    assert out == process_ref


@pytest.fixture(
    params=[
        ({'data': {'from_node': 'cube'}, 'parameters': (0,1,0.5), 'function': {'from_node': 'udf'}, 'dimension': 't'},
         "{'data': _cube, 'parameters': (0, 1, 0.5), 'function': _udf, 'dimension': 't'}"),
        ({'data': {'from_node': 'cube'}, 'parameters': {'from_node': 'data_2'}, 'function': {'from_node': 'udf'}, 'dimension': 't'},
         "{'data': _cube, 'parameters': _data_2, 'function': _udf, 'dimension': 't'}"),
        ({'data': {'from_node': 'cube'}, 'parameters': {'from_node': 'data_2'}, 'function': {'from_node': 'udf'}, 'dimension': 't', 'labels': None},
         "{'data': _cube, 'parameters': _data_2, 'function': _udf, 'dimension': 't', 'labels': None}"),
        ({'data': {'from_node': 'cube'}, 'parameters': {'from_node': 'data_2'}, 'function': {'from_node': 'udf'}, 'dimension': 't', 'labels': '2000-01-01T00:00:00Z'},
         "{'data': _cube, 'parameters': _data_2, 'function': _udf, 'dimension': 't', 'labels': '2000-01-01T00:00:00Z'}"),
        ({'data': {'from_node': 'cube'}, 'parameters': {'from_node': 'data_2'}, 'function': {'from_node': 'udf'}, 'dimension': 't', 'labels': ['2000-01-01T00:00:00Z', '2000-01-02T00:00:00Z']},
         "{'data': _cube, 'parameters': _data_2, 'function': _udf, 'dimension': 't', 'labels': ['2000-01-01T00:00:00Z', '2000-01-02T00:00:00Z']}"),
    ]
)
def get_fit_param(request):
    return create_params("node_curve", request.param[0], request.param[1])

@pytest.mark.parametrize("process", (
        "fit_curve",
        "predict_curve",
))
def test_curve(process: str, get_fit_param: Tuple[str, Dict[str, Any], str]):
    """Test conversions of processes with input x, y."""
    node_id, process_def, process_ref = set_process(*get_fit_param, process)

    out = map_general(node_id, process_def)

    assert out == process_ref
