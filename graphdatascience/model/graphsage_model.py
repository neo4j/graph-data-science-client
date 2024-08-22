from typing import Any

from pandas import Series

from ..call_parameters import CallParameters
from ..graph.graph_object import Graph
from ..graph.graph_type_check import graph_type_check
from .model import Model


class GraphSageModel(Model):
    """
    Represents a GraphSAGE model in the model catalog.
    Construct this using :func:`gds.beta.graphSage.train()`.
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
        endpoint = self._endpoint_prefix() + "write"
        config["modelName"] = self.name()
        params = CallParameters(graph_name=G.name(), config=config)

        return self._query_runner.call_procedure(  # type: ignore
            endpoint=endpoint, params=params, logging=True
        ).squeeze()

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
        return self._estimate_predict("write", G.name(), config)
