from typing import Any, Dict, List

from ..model.link_prediction_model import LPModel
from ..query_runner.query_runner import QueryRunner, Row
from .training_pipeline import TrainingPipeline


class LPTrainingPipeline(TrainingPipeline):
    def addFeature(self, feature_type: str, **config: Any) -> Row:
        query = (
            f"{self._query_prefix()}addFeature($pipeline_name, $feature_type, $config)"
        )
        params = {
            "pipeline_name": self.name(),
            "feature_type": feature_type,
            "config": config,
        }

        return self._query_runner.run_query(query, params)[0]

    def feature_steps(self) -> List[Dict[str, Any]]:
        return self._list_info()["pipelineInfo"]["featurePipeline"]["featureSteps"]  # type: ignore

    def _query_prefix(self) -> str:
        return "CALL gds.alpha.ml.pipeline.linkPrediction."

    def _create_trained_model(self, name: str, query_runner: QueryRunner) -> LPModel:
        return LPModel(name, query_runner)
