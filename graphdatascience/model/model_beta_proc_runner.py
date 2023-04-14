from typing import Any, Optional

from pandas import DataFrame, Series

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from .model import Model


class ModelBetaProcRunner(UncallableNamespace, IllegalAttrChecker):
    def list(self, model: Optional[Model] = None) -> DataFrame:
        """
        List all models or a model matching the name of the specified model.

        Args:
            model (Model, optional): the model to list, or None to list all models.

        Returns:
            a DataFrame containing information about the models in the model catalog.
        """
        self._namespace += ".list"

        if model:
            query = f"CALL {self._namespace}($model_name)"
            params = {"model_name": model.name()}
        else:
            query = f"CALL {self._namespace}()"
            params = {}

        return self._query_runner.run_query(query, params)

    def exists(self, model_name: str) -> "Series[Any]":
        """
        Check if a model of a given name exists in the Model Catalog.

        Args:
            model_name (str): the name of the model to check.

        Returns:
            a Series containing a boolean value indicating whether the model exists.
        """
        self._namespace += ".exists"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model_name}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def drop(self, model: Model) -> "Series[Any]":
        """
        Drop a model from the Model Catalog.

        Args:
            model (Model): the model to drop.

        Returns:
            a Series containing the result of the drop operation.
        """
        self._namespace += ".drop"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model.name()}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore
