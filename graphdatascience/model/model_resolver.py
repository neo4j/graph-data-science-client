from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..model.link_prediction_model import LPModel
from ..model.node_classification_model import NCModel
from ..model.node_regression_model import NRModel
from .graphsage_model import GraphSageModel
from .model import Model


class ModelResolver(UncallableNamespace, IllegalAttrChecker):
    def _resolve_model(self, model_type: str, model_name: str) -> Model:
        if model_type == "NodeClassification":
            return NCModel(model_name, self._query_runner, self._server_version)
        elif model_type == "LinkPrediction":
            return LPModel(model_name, self._query_runner, self._server_version)
        elif model_type == "NodeRegression":
            return NRModel(model_name, self._query_runner, self._server_version)
        elif model_type == "graphSage":
            return GraphSageModel(model_name, self._query_runner, self._server_version)

        raise ValueError(f"Unknown model type encountered: '{model_type}'")
