from typing import Any

import neo4j

from graphdatascience.call_parameters import CallParameters
from graphdatascience.model.v2.model_api import ModelApi
from graphdatascience.model.v2.model_details import ModelDetails
from graphdatascience.query_runner.query_runner import QueryRunner


class ModelApiCypher(ModelApi):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner
        super().__init__()

    def exists(self, model: str) -> bool:
        params = CallParameters(name=model)

        result = self._query_runner.call_procedure("gds.model.exists", params=params, custom_error=False)
        if result.empty:
            return False

        return result.iloc[0]["exists"]  # type: ignore

    def get(self, model: str) -> ModelDetails:
        params = CallParameters(name=model)

        result = self._query_runner.call_procedure("gds.model.list", params=params, custom_error=False)
        if result.empty:
            raise ValueError(f"There is no '{model}' in the model catalog")

        return self._to_model_details(result.iloc[0].to_dict())

    def drop(self, model: str, fail_if_missing: bool) -> ModelDetails | None:
        params = CallParameters(model_name=model, fail_if_missing=fail_if_missing)

        result = self._query_runner.call_procedure("gds.model.drop", params=params, custom_error=False)

        if result.empty:
            return None

        return self._to_model_details(result.iloc[0].to_dict())

    def _to_model_details(self, result: dict[str, Any]) -> ModelDetails:
        creation_time = result.get("creationTime", None)
        if creation_time and isinstance(creation_time, neo4j.time.DateTime):
            result["creationTime"] = creation_time.to_native()

        return ModelDetails(**result)
