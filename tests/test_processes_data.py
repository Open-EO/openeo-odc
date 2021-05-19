"""
Tests processes with input type func(data, **kwargs)
"""

from pytest import mark
from openeo_odc.map_processes_odc import map_data


@mark.parametrize("node_id, process_def, kwargs, process_ref",
    [
        (
            "nir_1",
            {'process_id': 'array_element', 'arguments': {'data': 'dc_0', 'index': 0}},
            {'from_parameter': {'data': 'dc_0'}, 'dimension': 'spectral'},
            "nir_1 = oeop.array_element(**{'data': dc_0, 'index': 0, 'dimension': 'bands'})\n"
        ),
        (
            "nir_2",
            {'process_id': 'array_element', 'arguments': {'data': 'dc_0', 'label': 'nir'}},
            {'from_parameter': {'data': 'dc_0'}, 'dimension': 'spectral'},
            "nir_2 = oeop.array_element(**{'data': dc_0, 'label': 'nir', 'dimension': 'bands'})\n"
        ),
        (
            "nir_3",
            {'process_id': 'array_element', 'arguments': {'data': 'dc_0', 'label': 'nir', 'return_nodata': True}},
            {'from_parameter': {'data': 'dc_0'}, 'dimension': 'spectral'},
            "nir_3 = oeop.array_element(**{'data': dc_0, 'label': 'nir', 'return_nodata': True, 'dimension': 'bands'})\n"
        ),
    ]
)
def test_array_element(node_id, process_def, kwargs, process_ref):
    """Test all `array_element` conversions."""

    out = map_data(node_id, process_def, kwargs)
    assert out == process_ref
