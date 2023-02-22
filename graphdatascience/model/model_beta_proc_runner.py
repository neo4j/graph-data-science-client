from typing import Any, Optional

from pandas import DataFrame, Series

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from .model import Model


class ModelBetaProcRunner(UncallableNamespace, IllegalAttrChecker):
    def list(self, model: Optional[Model] = None) -> DataFrame:
        self._namespace += ".list"

        if model:
            query = f"CALL {self._namespace}($model_name)"
            params = {"model_name": model.name()}
        else:
            query = f"CALL {self._namespace}()"
            params = {}

        return self._query_runner.run_query(query, params)

    def exists(self, model_name: str) -> "Series[Any]":
        self._namespace += ".exists"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model_name}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def drop(self, model: Model) -> "Series[Any]":
        self._namespace += ".drop"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model.name()}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore
