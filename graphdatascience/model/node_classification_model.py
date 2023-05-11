from typing import Any, Dict, List

from pandas import Series

from ..graph.graph_object import Graph
from ..graph.graph_type_check import graph_type_check
from .pipeline_model import PipelineModel


class NCModel(PipelineModel):
    """
    Represents a node classification model in the model catalog.
    Construct this using
    :func:`NCTrainingPipeline.train() <graphdatascience.pipeline.nc_training_pipeline.NCTrainingPipeline.train>`.
    """

    def _query_prefix(self) -> str:
        return "CALL gds.beta.pipeline.nodeClassification.predict."

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
        query = f"{self._query_prefix()}write($graph_name, $config)"
        config["modelName"] = self.name()
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query_with_logging(query, params).squeeze()  # type: ignore

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

    def classes(self) -> List[int]:
        """
        Get the classes of the model.

        Returns:
            The classes of the model.

        """
        return self._list_info()["modelInfo"][0]["classes"]  # type: ignore

    def feature_properties(self) -> List[str]:
        """
        Get the feature properties of the model.

        Returns:
            The feature properties of the model.

        """
        features: List[Dict[str, Any]] = self._list_info()["modelInfo"][0]["pipeline"]["featureProperties"]
        return [f["feature"] for f in features]
