"""
Tests processes with input type func(data, **kwargs)
"""
import pytest
from pytest import mark
from openeo_odc.map_processes_odc import map_general

from utils import create_params


@mark.parametrize("node_id, process_def, kwargs, process_ref",
    [
        (
            "nir_1",
            {'process_id': 'array_element', 'arguments': {'data': {'from_parameter': 'data'}, 'index': 0}},
            {'from_parameter': {'data': 'dc_0'}, 'dimension': 'spectral'},
            "_nir_1 = oeop.array_element(**{'data': _dc_0, 'index': 0, 'dimension': 'bands'})\n"
        ),
        (
            "nir_2",
            {'process_id': 'array_element', 'arguments': {'data': {'from_parameter': 'data'}, 'label': 'nir'}},
            {'from_parameter': {'data': 'dc_0'}, 'dimension': 'spectral'},
            "_nir_2 = oeop.array_element(**{'data': _dc_0, 'label': 'nir', 'dimension': 'bands'})\n"
        ),
        (
            "nir_3",
            {'process_id': 'array_element', 'arguments': {'data': {'from_parameter': 'data'}, 'label': 'nir', 'return_nodata': True}},
            {'from_parameter': {'data': 'dc_0'}, 'dimension': 'spectral'},
            "_nir_3 = oeop.array_element(**{'data': _dc_0, 'label': 'nir', 'return_nodata': True, 'dimension': 'bands'})\n"
        ),
        (
            "fit_1",
            {'process_id': 'fit_curve', 'arguments': {'data': {'from_parameter': 'data'}, 'parameters': (0, 1, 0.5), 'function': {'from_parameter': 'function'}}},
            {'from_parameter': {'data': 'dc_0', 'function': 'udf'}, 'dimension': 'time'},
            "_fit_1 = oeop.fit_curve(**{'data': _dc_0, 'parameters': (0, 1, 0.5), 'function': _udf, 'dimension': 'time'})\n"
        ),
    ]
)
def test_array_element(node_id, process_def, kwargs, process_ref):
    """Test all `array_element` conversions."""

    out = map_general(node_id, process_def, kwargs)
    assert out == process_ref
