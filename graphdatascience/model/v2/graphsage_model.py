from typing import Any

from pandas import Series
from pydantic import BaseModel
from pydantic.alias_generators import to_camel

from ...graph.graph_object import Graph
from ...graph.graph_type_check import graph_type_check
from .model import Model


class GraphSageModelV2(Model):
    """
    Represents a GraphSAGE model in the model catalog.
    Construct this using :func:`gds.graphSage.train()`.
    """

    def _endpoint_prefix(self) -> str:
        return "gds.beta.graphSage."

    @graph_type_check
    def predict_write(self, G: Graph, **config: Any) -> "Series[Any]":
        """
        Generate embeddings for the given graph and write the results to the database.

        Args:
            G: The graph to generate embeddings for.
            **config: The config for the prediction.

        Returns:
            The result of the write operation.

        """
        raise ValueError

    @graph_type_check
    def predict_write_estimate(self, G: Graph, **config: Any) -> "Series[Any]":
        """
        Estimate the memory needed to generate embeddings for the given graph and write the results to the database.

        Args:
            G: The graph to generate embeddings for.
            **config: The config for the prediction.

        Returns:
            The memory needed to generate embeddings for the given graph and write the results to the database.

        """
        raise ValueError


class GraphSageMutateResult(BaseModel, alias_generator=to_camel):
    node_count: int
    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    mutate_millis: int
    configuration: dict[str, Any]

    def __getitem__(self, item: str) -> Any:
        return self.__dict__[item]


class GraphSageWriteResult(BaseModel, alias_generator=to_camel):
    node_count: int
    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    configuration: dict[str, Any]

    def __getitem__(self, item: str) -> Any:
        return self.__dict__[item]
