class ExtraFuncUtils:

    def get_func_name(self, node_id: str) -> str:
        return f"extra_func_{node_id}"

    def get_dict_key(self, node_id: str) -> str:
        return f"extra_{node_id}"

    def get_func_header(self, node_id) -> str:
        return f"\n\ndef {self.get_func_name(node_id)}(x, *parameters):\n"


SUFFIXED_PROCESSES = [
    "and",
    "any",
    "all",
    "if",
    "or",
]

def get_py_process_name(process: str) -> str:
    """Return correct process name as in `openeo-processes-python` repo."""
    if process in SUFFIXED_PROCESSES:
        process += "_"
    return process


def get_oeop_str(id, process_name, params_str) -> str:
    """Return `openeo-processes-python` call."""
    oeop_name = get_py_process_name(process_name)
    return f"""{'_'+id} = oeop.{oeop_name}({params_str})
"""


# Some processes require an additional function definition
# their chil processes should not map the function parameters but rather handle them as variables
PROCESSES_WITH_VARIABLES = {
    "fit_curve": ["x", "parameters"],
    "predict_curve": ["x", "parameters"],
}
