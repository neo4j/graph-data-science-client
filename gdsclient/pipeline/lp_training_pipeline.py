from typing import Any, Dict, List

from ..query_runner.query_runner import QueryRunner
from .lp_prediction_pipeline import LPPredictionPipeline
from .training_pipeline import TrainingPipeline


class LPTrainingPipeline(TrainingPipeline):
    def __init__(self, name: str, query_runner: QueryRunner):
        super().__init__(name, query_runner)

    def addFeature(self, feature_type: str, **config: Any) -> None:
        query = (
            f"{self._query_prefix()}addFeature($pipeline_name, $feature_type, $config)"
        )
        params = {
            "pipeline_name": self.name(),
            "feature_type": feature_type,
            "config": config,
        }
        self._query_runner.run_query(query, params)

    def feature_steps(self) -> List[Dict[str, Any]]:
        return self._list_info()["modelInfo"]["featurePipeline"]["featureSteps"]  # type: ignore

    def _query_prefix(self) -> str:
        return "CALL gds.alpha.ml.pipeline.linkPrediction."

    def _create_trained_model(
        self, name: str, query_runner: QueryRunner
    ) -> LPPredictionPipeline:
        return LPPredictionPipeline(name, query_runner)
