"""
"""

from openeo_odc.map_processes_odc import map_data


def test_array_element():
    """Test all array_element conversions."""

    process_defs = [
        (
         'nir_1',
         {'process_id': 'array_element', 'arguments': {'data': 'dc_0', 'index': 0}},
         {'from_parameter': {'data': 'dc_0'}, 'dimension': 'spectral'}
        ),
        (
         'nir_2',
         {'process_id': 'array_element', 'arguments': {'data': 'dc_0', 'label': 'nir'}},
         {'from_parameter': {'data': 'dc_0'}, 'dimension': 'spectral'}
        ),
        (
         'nir_3',
         {'process_id': 'array_element', 'arguments': {'data': 'dc_0', 'label': 'nir', 'return_nodata': True}},
         {'from_parameter': {'data': 'dc_0'}, 'dimension': 'spectral'}
        )
    ]
    process_refs = [
        "nir_1 = oeop.array_element(**{'data': dc_0, 'index': 0, 'dimension': 'bands'})\n",
        "nir_2 = oeop.array_element(**{'data': dc_0, 'label': 'nir', 'dimension': 'bands'})\n",
        "nir_3 = oeop.array_element(**{'data': dc_0, 'label': 'nir', 'return_nodata': True, 'dimension': 'bands'})\n"
    ]

    for k, process_def in enumerate(process_defs):
        out = map_data(process_def[0], process_def[1], process_def[2])
        assert out == process_refs[k]
