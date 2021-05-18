"""
Tests processes with input type func(x, y)
"""

from pytest import mark
from openeo_odc.map_processes_odc import map_xy


@mark.parametrize("node_id, process_def, process_ref",
    [
        (
            "multiply_1",
            {'process_id': 'multiply', 'arguments': {'x': 3, 'y': 6}},
            "multiply_1 = oeop.multiply(**{'x': 3,'y': 6})\n"
        ),
        (
            "multiply_2",
            {'process_id': 'multiply', 'arguments': {'x': {'from_node': 'red_3'}, 'y': 6}},
            "multiply_2 = oeop.multiply(**{'x': red_3,'y': 6})\n"
        ),
        (
            "multiply_3",
            {'process_id': 'multiply', 'arguments': {'x': {'from_node': 'red_3'}, 'y': {'from_node': 'blue_2'}}},
            "multiply_3 = oeop.multiply(**{'x': red_3,'y': blue_2})\n"
        )
    ]
)
def test_multiply(node_id, process_def, process_ref):
    """Test all multiply conversions."""

    out = map_xy(node_id, process_def)
    assert out == process_ref
