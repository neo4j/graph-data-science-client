from __future__ import annotations

from typing import Any

from pandas import DataFrame, Series

from ..call_parameters import CallParameters
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from .model import Model


class ModelBetaProcRunner(UncallableNamespace, IllegalAttrChecker):
    def list(self, model: Model | None = None) -> DataFrame:
        self._namespace += ".list"

        params = CallParameters()
        if model:
            params["model_name"] = model.name()

        return self._query_runner.call_procedure(endpoint=self._namespace, params=params)

    def exists(self, model_name: str) -> Series[Any]:
        self._namespace += ".exists"
        params = CallParameters(model_name=model_name)

        return self._query_runner.call_procedure(endpoint=self._namespace, params=params).squeeze()  # type: ignore

    def drop(self, model: Model) -> Series[Any]:
        self._namespace += ".drop"
        params = CallParameters(model_name=model.name())

        return self._query_runner.call_procedure(endpoint=self._namespace, params=params).squeeze()  # type: ignore
