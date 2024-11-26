from __future__ import annotations

from typing import Any, Optional, Union

from pandas import DataFrame, Series

from ..call_parameters import CallParameters
from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..graph.graph_object import Graph
from ..model.simple_rel_embedding_model import SimpleRelEmbeddingModel
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion
from .model import Model
from .model_resolver import ModelResolver


class DistMultCreator(UncallableNamespace, IllegalAttrChecker):
    @compatible_with("create", min_inclusive=ServerVersion(2, 5, 0))
    @client_only_endpoint("gds.model.distmult")
    def create(
        self, G: Graph, node_embedding_property: str, relationship_type_embeddings: dict[str, list[float]]
    ) -> SimpleRelEmbeddingModel:
        return SimpleRelEmbeddingModel(
            "distmult",
            self._query_runner,
            self._server_version,
            G.name(),
            node_embedding_property,
            relationship_type_embeddings,
        )


class TransECreator(UncallableNamespace, IllegalAttrChecker):
    @compatible_with("create", min_inclusive=ServerVersion(2, 5, 0))
    @client_only_endpoint("gds.model.transe")
    def create(
        self, G: Graph, node_embedding_property: str, relationship_type_embeddings: dict[str, list[float]]
    ) -> SimpleRelEmbeddingModel:
        return SimpleRelEmbeddingModel(
            "transe",
            self._query_runner,
            self._server_version,
            G.name(),
            node_embedding_property,
            relationship_type_embeddings,
        )


class ModelProcRunner(ModelResolver):
    @client_only_endpoint("gds.model")
    def get(self, model_name: str) -> Model:
        params = CallParameters(model_name=model_name)
        if self._server_version < ServerVersion(2, 5, 0):
            endpoint = "gds.beta.model.list"
            yields = ["modelInfo"]
            result_25: Series[Any] = self._query_runner.call_procedure(
                endpoint=endpoint, params=params, yields=yields, custom_error=False
            ).squeeze()
            model_type = str(result_25["modelInfo"]["modelType"]) if not result_25.empty else None
        else:
            endpoint = "gds.model.list"
            yields = ["modelType"]
            result: Union[str, Series[Any]] = self._query_runner.call_procedure(
                endpoint=endpoint, params=params, yields=yields, custom_error=False
            ).squeeze()
            model_type = result if isinstance(result, str) else None

        if model_type is None:
            raise ValueError(f"No loaded model named '{model_name}' exists")

        return self._resolve_model(model_type, model_name)

    @compatible_with("store", min_inclusive=ServerVersion(2, 5, 0))
    def store(self, model: Model, failIfUnsupportedType: bool = True) -> Series[Any]:
        self._namespace += ".store"
        params = CallParameters(model_name=model.name(), fail_flag=failIfUnsupportedType)

        return self._query_runner.call_procedure(  # type: ignore
            endpoint=self._namespace,
            params=params,
        ).squeeze()

    @compatible_with("publish", min_inclusive=ServerVersion(2, 5, 0))
    def publish(self, model: Model) -> Model:
        self._namespace += ".publish"

        params = CallParameters(model_name=model.name())

        result = self._query_runner.call_procedure(endpoint=self._namespace, params=params)

        model_name = result["modelName"][0]
        model_type = result["modelType"][0]

        return self._resolve_model(model_type, model_name)

    @compatible_with("load", min_inclusive=ServerVersion(2, 5, 0))
    def load(self, model_name: str) -> tuple[Model, Series[Any]]:
        self._namespace += ".load"

        params = CallParameters(model_name=model_name)

        result = self._query_runner.call_procedure(endpoint=self._namespace, params=params).squeeze()

        self._namespace = "gds.model"
        proc_runner = ModelProcRunner(self._query_runner, self._namespace, self._server_version)

        return proc_runner.get(result["modelName"]), result

    @compatible_with("delete", min_inclusive=ServerVersion(2, 5, 0))
    def delete(self, model: Model) -> Series[Any]:
        self._namespace += ".delete"
        params = CallParameters(model_name=model.name())
        return self._query_runner.call_procedure(endpoint=self._namespace, params=params).squeeze()  # type: ignore

    @compatible_with("list", min_inclusive=ServerVersion(2, 5, 0))
    def list(self, model: Optional[Model] = None) -> DataFrame:
        self._namespace += ".list"

        params = CallParameters()
        if model:
            params["model_name"] = model.name()

        return self._query_runner.call_procedure(endpoint=self._namespace, params=params)

    @compatible_with("exists", min_inclusive=ServerVersion(2, 5, 0))
    def exists(self, model_name: str) -> Series[Any]:
        self._namespace += ".exists"

        return self._query_runner.call_procedure(  # type: ignore
            endpoint=self._namespace, params=CallParameters(model_name=model_name)
        ).squeeze()

    @compatible_with("drop", min_inclusive=ServerVersion(2, 5, 0))
    def drop(self, model: Model) -> Series[Any]:
        self._namespace += ".drop"

        return self._query_runner.call_procedure(  # type: ignore
            endpoint=self._namespace, params=CallParameters(model_name=model.name())
        ).squeeze()

    @property
    def transe(self) -> TransECreator:
        self._namespace += ".transe"
        return TransECreator(self._query_runner, self._namespace, self._server_version)

    @property
    def distmult(self) -> DistMultCreator:
        self._namespace += ".distmult"
        return DistMultCreator(self._query_runner, self._namespace, self._server_version)
