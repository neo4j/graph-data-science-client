from abc import ABC
from typing import Any, Union

from pandas import Series

from ..call_parameters import CallParameters
from ..model.node_classification_model import NCModel
from ..pipeline.classification_training_pipeline import ClassificationTrainingPipeline
from ..query_runner.query_runner import QueryRunner


class NCTrainingPipeline(ClassificationTrainingPipeline[NCModel], ABC):
    """
    Represents a node classification training pipeline.
    Construct an instance of this class using :func:`graphdatascience.GraphDataScience.nc_pipe`.
    """

    def selectFeatures(self, node_properties: Union[str, list[str]]) -> "Series[Any]":
        """
        Select the node properties to use for training.

        Args:
            node_properties: The node properties to use for training.

        Returns:
            The result of the query.

        """
        endpoint = f"{self._endpoint_prefix()}selectFeatures"
        params = CallParameters(pipeline_name=self.name(), node_properties=node_properties)

        return self._query_runner.call_procedure(endpoint=endpoint, params=params).squeeze()  # type: ignore

    def feature_properties(self) -> "Series[Any]":
        """
        Get the feature properties of the pipeline.

        Returns:
            A Series containing the feature properties of the pipeline.

        """
        pipeline_info = self._list_info()["pipelineInfo"][0]
        feature_properties: "Series[Any]" = Series(pipeline_info["featurePipeline"]["featureProperties"], dtype=object)
        return feature_properties

    def _endpoint_prefix(self) -> str:
        return "gds.beta.pipeline.nodeClassification."

    def _create_trained_model(self, name: str, query_runner: QueryRunner) -> NCModel:
        return NCModel(name, query_runner, self._server_version)
