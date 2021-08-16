class FitCurveUtils:

    def get_func_name(self, fc_node_id: str) -> str:
        return f"fit_curve_func_{fc_node_id}"

    def get_dict_key(self, fc_node_id: str) -> str:
        return f"fit_curve_{fc_node_id}"

    def get_func_header(self, fc_node_id) -> str:
        return f"\n\ndef {self.get_func_name(fc_node_id)}(x, *parameters):\n"
