from typing import Any

from pandas import DataFrame, Series

from ..call_parameters import CallParameters
from ..model.link_prediction_model import LPModel
from ..query_runner.query_runner import QueryRunner
from .classification_training_pipeline import ClassificationTrainingPipeline


class LPTrainingPipeline(ClassificationTrainingPipeline[LPModel]):
    """
    Represents a link prediction training pipeline.
    Construct an instance of this class using :func:`graphdatascience.GraphDataScience.lp_pipe`.
    """

    def addFeature(self, feature_type: str, **config: Any) -> "Series[Any]":
        """
        Add a link feature to the pipeline.

        Args:
            feature_type: The type of feature to add.
            **config: The configuration for the feature, this includes the node properties to use.

        Returns:
            The result of the query.
        """
        endpoint = f"{self._endpoint_prefix()}addFeature"
        params = CallParameters(
            pipeline_name=self.name(), feature_type=feature_type, config=self._expand_ranges(config)
        )

        return self._query_runner.call_procedure(endpoint=endpoint, params=params).squeeze()  # type: ignore

    def feature_steps(self) -> DataFrame:
        """
        Get the feature steps of the pipeline.

        Returns:
            A DataFrame containing the feature steps of the pipeline.

        """
        pipeline_info = self._list_info()["pipelineInfo"][0]
        return DataFrame(pipeline_info["featurePipeline"]["featureSteps"])

    def _endpoint_prefix(self) -> str:
        return "gds.beta.pipeline.linkPrediction."

    def _create_trained_model(self, name: str, query_runner: QueryRunner) -> LPModel:
        return LPModel(name, query_runner, self._server_version)
