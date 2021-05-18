"""
"""

from openeo_odc.map_processes_odc import map_xy


def test_processes_xy():
    """Map processes with input type f(x, y)."""

    multiply()


def multiply():
    """Test all multiply conversions."""

    process_defs = [
        ('mutplipy_1', {'process_id': 'multiply', 'arguments': {'x': 3, 'y': 6}}),
        ('mutplipy_2', {'process_id': 'multiply', 'arguments': {'x': {'from_node': 'red_3'}, 'y': 6}}),
        ('mutplipy_3', {'process_id': 'multiply', 'arguments': {'x': {'from_node': 'red_3'}, 'y': {'from_node': 'blue_2'}}})
    ]
    process_refs = [
        "mutplipy_1 = oeop.multiply(**{'x': 3,'y': 6})\n",
        "mutplipy_2 = oeop.multiply(**{'x': red_3,'y': 6})\n",
        "mutplipy_3 = oeop.multiply(**{'x': red_3,'y': blue_2})\n"
    ]

    for k, process_def in enumerate(process_defs):
        out = map_xy(process_def[0], process_def[1])
        assert out == process_refs[k]
