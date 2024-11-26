from typing import Any

from pandas import Series

from ..call_parameters import CallParameters
from ..graph.graph_object import Graph
from ..graph.graph_type_check import graph_type_check
from .pipeline_model import PipelineModel


class NCModel(PipelineModel):
    """
    Represents a node classification model in the model catalog.
    Construct this using
    :func:`NCTrainingPipeline.train() <graphdatascience.pipeline.nc_training_pipeline.NCTrainingPipeline.train>`.
    """

    def _endpoint_prefix(self) -> str:
        return "gds.beta.pipeline.nodeClassification.predict."

    @graph_type_check
    def predict_write(self, G: Graph, **config: Any) -> "Series[Any]":
        """
        Predict the node labels of a graph and write the results to the database.

        Args:
            G: The graph to predict on.
            **config: The config for the prediction.

        Returns:
            The result of the write operation.

        """
        endpoint = f"{self._endpoint_prefix()}write"
        config["modelName"] = self.name()
        params = CallParameters(graph_name=G.name(), config=config)

        return self._query_runner.call_procedure(  # type: ignore
            endpoint=endpoint, params=params, logging=True
        ).squeeze()

    @graph_type_check
    def predict_write_estimate(self, G: Graph, **config: Any) -> "Series[Any]":
        """
        Estimate the memory needed to predict the node labels of a graph and write the results to the database.

        Args:
            G: The graph to predict on.
            **config: The config for the prediction.

        Returns:
            The memory needed to predict the node labels of a graph and write the results to the database.

        """
        return self._estimate_predict("write", G.name(), config)

    def classes(self) -> list[int]:
        """
        Get the classes of the model.

        Returns:
            The classes of the model.

        """
        return self._list_info()["modelInfo"]["classes"]  # type: ignore

    def feature_properties(self) -> list[str]:
        """
        Get the feature properties of the model.

        Returns:
            The feature properties of the model.

        """
        features: list[dict[str, Any]] = self._list_info()["modelInfo"]["pipeline"]["featureProperties"]
        return [f["feature"] for f in features]
