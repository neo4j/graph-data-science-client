from abc import ABC
from typing import Any, Dict, Tuple
from uuid import uuid4
from warnings import warn

from pandas import DataFrame, Series

from ..call_parameters import CallParameters
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..graph.graph_entity_ops_runner import (
    GraphElementPropertyRunner,
    GraphNodePropertiesRunner,
    GraphRelationshipsRunner,
)
from ..graph.graph_object import Graph
from ..graph.graph_type_check import graph_type_check
from ..model.graphsage_model import GraphSageModel
from ..query_runner.arrow_query_runner import ArrowQueryRunner


class AlgoProcRunner(IllegalAttrChecker, ABC):
    @graph_type_check
    def _run_procedure(self, G: Graph, config: Dict[str, Any], with_logging: bool = True) -> DataFrame:
        params = CallParameters(graph_name=G.name(), config=config)

        return self._query_runner.call_procedure(endpoint=self._namespace, params=params, logging=with_logging)

    @graph_type_check
    def estimate(self, G: Graph, **config: Any) -> "Series[Any]":
        self._namespace += "." + "estimate"
        return self._run_procedure(G, config, with_logging=False).squeeze()  # type: ignore


class StreamModeRunner(AlgoProcRunner):
    def __call__(self, G: Graph, stream_with_arrow: bool = False, **config: Any) -> DataFrame:
        if stream_with_arrow:
            if not isinstance(self._query_runner, ArrowQueryRunner):
                raise ValueError("The `stream_with_arrow` option requires GDS EE with the Arrow server enabled")

            try:
                return self._stream_with_arrow(G, config)
            except Exception as e:
                warn(
                    "Falling back to streaming with Neo4j driver, "
                    f"since failed to stream with Arrow with reason: {str(e)}"
                )

                del config["mutateProperty"]
                del config["mutateRelationshipType"]

                self._namespace = self._namespace.replace("mutate", "stream")
                return self._run_procedure(G, config)
        else:
            return self._run_procedure(G, config)

    def _stream_with_arrow(self, G: Graph, config: Dict[str, Any]) -> DataFrame:
        self._namespace = self._namespace.replace("stream", "mutate")

        mutate_property = str(uuid4())
        config["mutateProperty"] = mutate_property
        try:
            self._run_procedure(G, config)

            elem_prop_runner = GraphElementPropertyRunner(
                self._query_runner, "gds.graph.nodeProperty", self._server_version
            )
            if "concurrency" in config:
                result = elem_prop_runner.stream(G, mutate_property, concurrency=config["concurrency"])
            else:
                result = elem_prop_runner.stream(G, mutate_property)

            node_prop_runner = GraphNodePropertiesRunner(
                self._query_runner, "gds.graph.nodeProperties", self._server_version
            )
            node_prop_runner.drop(G, [mutate_property])

            return result
        except Exception as e:
            if "No value specified for the mandatory configuration parameter `mutateRelationshipType`" not in str(e):
                raise e

            mutate_relationship_type = str(uuid4())
            config["mutateRelationshipType"] = mutate_relationship_type
            self._run_procedure(G, config)

            elem_prop_runner = GraphElementPropertyRunner(
                self._query_runner, "gds.graph.relationshipProperty", self._server_version
            )
            if "concurrency" in config:
                result = elem_prop_runner.stream(
                    G, mutate_property, [mutate_relationship_type], concurrency=config["concurrency"]
                )
            else:
                result = elem_prop_runner.stream(G, mutate_property, [mutate_relationship_type])

            rel_runner = GraphRelationshipsRunner(self._query_runner, "gds.graph.relationships", self._server_version)
            rel_runner.drop(G, mutate_relationship_type)

            return result.drop("relationshipType", axis=1)


class StandardModeRunner(AlgoProcRunner):
    def __call__(self, G: Graph, **config: Any) -> "Series[Any]":
        return self._run_procedure(G, config).squeeze()  # type: ignore


class GraphSageRunner(AlgoProcRunner):
    @graph_type_check
    def __call__(self, G: Graph, **config: Any) -> Tuple[GraphSageModel, "Series[Any]"]:
        result = self._run_procedure(G, config).squeeze()
        model_name = result["modelInfo"]["modelName"]

        return GraphSageModel(model_name, self._query_runner, self._server_version), result
