from typing import Any, Tuple

from pandas import Series

from .model import Model
from .model_proc_runner import ModelProcRunner
from .model_resolver import ModelResolver


class ModelAlphaProcRunner(ModelResolver):
    def store(self, model: Model, failIfUnsupportedType: bool = True) -> "Series[Any]":
        """
        Persist a model from the Model Catalog to disk.

        Args:
            model (Model): a model object representing the model to persist.
            failIfUnsupportedType (bool, optional): whether to fail if the model cannot be persisted.

        Returns:
            a Series containing the result of the store operation.
        """
        self._namespace += ".store"

        query = f"CALL {self._namespace}($model_name, $fail_flag)"
        params = {
            "model_name": model.name(),
            "fail_flag": failIfUnsupportedType,
        }

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def publish(self, model: Model) -> Model:
        """
        Publish a model from the Model Catalog so that other users can access it.

        Args:
            model (Model): a model object representing the model to publish.

        Returns:
            a Model object representing the published model.
        """
        self._namespace += ".publish"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model.name()}

        result = self._query_runner.run_query(query, params)

        model_name = result["modelInfo"][0]["modelName"]
        model_type = result["modelInfo"][0]["modelType"]

        return self._resolve_model(model_type, model_name)

    def load(self, model_name: str) -> Tuple[Model, "Series[Any]"]:
        """
        Load a model from disk into the Model Catalog.

        Args:
            model_name (str): the name of the model to load.

        Returns:
            a tuple containing a Model object representing the loaded model
            and a Series containing the result of the load operation.
        """
        self._namespace += ".load"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model_name}

        result = self._query_runner.run_query(query, params).squeeze()

        self._namespace = "gds.model"
        proc_runner = ModelProcRunner(self._query_runner, self._namespace, self._server_version)

        return proc_runner.get(result["modelName"]), result

    def delete(self, model: Model) -> "Series[Any]":
        """
        Delete a stored model from disk.

        Args:
            model (Model): a model object representing the model to delete.

        Returns:
            a Series containing the result of the delete operation.
        """
        self._namespace += ".delete"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model.name()}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore
