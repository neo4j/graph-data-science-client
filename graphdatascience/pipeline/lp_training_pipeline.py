from typing import Any

from pandas import DataFrame, Series

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
        query = f"{self._query_prefix()}addFeature($pipeline_name, $feature_type, $config)"
        params = {
            "pipeline_name": self.name(),
            "feature_type": feature_type,
            "config": config,
        }

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def feature_steps(self) -> DataFrame:
        """
        Get the feature steps of the pipeline.

        Returns:
            A DataFrame containing the feature steps of the pipeline.

        """
        pipeline_info = self._list_info()["pipelineInfo"][0]
        return DataFrame(pipeline_info["featurePipeline"]["featureSteps"])

    def _query_prefix(self) -> str:
        return "CALL gds.beta.pipeline.linkPrediction."

    def _create_trained_model(self, name: str, query_runner: QueryRunner) -> LPModel:
        return LPModel(name, query_runner, self._server_version)
