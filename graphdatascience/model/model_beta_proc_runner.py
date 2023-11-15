from typing import Any, Optional

from pandas import DataFrame, Series

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from .model import Model


class ModelBetaProcRunner(UncallableNamespace, IllegalAttrChecker):
    def list(self, model: Optional[Model] = None) -> DataFrame:
        self._namespace += ".list"

        if model:
            body = "$model_name"
            params = {"model_name": model.name()}
        else:
            body = None
            params = {}

        return self._query_runner.call_procedure(endpoint=self._namespace, body=body, params=params)

    def exists(self, model_name: str) -> "Series[Any]":
        self._namespace += ".exists"
        params = {"model_name": model_name}

        return self._query_runner.call_procedure(
            endpoint=self._namespace, body="$model_name", params=params
        ).squeeze()  # type: ignore

    def drop(self, model: Model) -> "Series[Any]":
        self._namespace += ".drop"
        params = {"model_name": model.name()}

        return self._query_runner.run_cypher(
            endpoint=self._namespace, body="$model_name", params=params
        ).squeeze()  # type: ignore
