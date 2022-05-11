from typing import Any

import pandas
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from ..model.link_prediction_model import LPModel
from ..query_runner.query_runner import QueryRunner
from .classification_training_pipeline import ClassificationTrainingPipeline


class LPTrainingPipeline(ClassificationTrainingPipeline):
    def addFeature(self, feature_type: str, **config: Any) -> Series:
        query = f"{self._query_prefix()}addFeature($pipeline_name, $feature_type, $config)"
        params = {
            "pipeline_name": self.name(),
            "feature_type": feature_type,
            "config": config,
        }

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def feature_steps(self) -> DataFrame:
        pipeline_info = self._list_info()["pipelineInfo"][0]
        return pandas.DataFrame(pipeline_info["featurePipeline"]["featureSteps"])

    def _query_prefix(self) -> str:
        return "CALL gds.beta.pipeline.linkPrediction."

    def _create_trained_model(self, name: str, query_runner: QueryRunner) -> LPModel:
        return LPModel(name, query_runner, self._server_version)
