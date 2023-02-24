from ..error.client_only_endpoint import client_only_endpoint
from .model import Model
from .model_resolver import ModelResolver


class ModelProcRunner(ModelResolver):
    @client_only_endpoint("gds.model")
    def get(self, model_name: str) -> Model:
        query = "CALL gds.beta.model.list($model_name)"
        params = {"model_name": model_name}
        result = self._query_runner.run_query(query, params, custom_error=False)

        if len(result) == 0:
            raise ValueError(f"No loaded model named '{model_name}' exists")

        model_type = result["modelInfo"][0]["modelType"]
        return self._resolve_model(model_type, model_name)
