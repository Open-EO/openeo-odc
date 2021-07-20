from typing import Any, Dict, Tuple


def create_params(node_id: str, args_in: Dict[str, Any], args_out: str) -> Tuple[str, Dict[str, Any], str]:
    """Format input and expected output of process."""
    return node_id, \
        {"process_id": "PLACEHOLDER", "arguments": args_in}, \
        f"_{node_id} = oeop.PLACEHOLDER(**{args_out})\n"


def set_process(node_id: str, process_def: Dict[str, Any], process_ref: str, process_name: str) \
        -> Tuple[str, Dict[str, Any], str]:
    """Set the PLACEHOLDER to the given process name."""
    process_def["process_id"] = process_name
    process_ref = process_ref.replace("PLACEHOLDER", process_name)
    return node_id, process_def, process_ref
