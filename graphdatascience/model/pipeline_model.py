from abc import ABC
from dataclasses import dataclass
from typing import Any, Dict

from pandas.core.series import Series

from .model import Model


@dataclass(frozen=True, repr=True)
class EvaluationScores:

    min: float
    avg: float
    max: float

    @staticmethod
    def create(raw_metrics: Dict[str, float]) -> "EvaluationScores":
        return EvaluationScores(raw_metrics["min"], raw_metrics["avg"], raw_metrics["max"])

    def __str__(self) -> str:
        return f"(min={self.min}, avg={self.avg}, max={self.max})"


@dataclass(frozen=True, repr=True)
class MetricScores:

    train: EvaluationScores
    validation: EvaluationScores
    outer_train: float
    test: float

    @staticmethod
    def create(raw_metrics: Dict[str, Any]) -> "MetricScores":
        train_eval = EvaluationScores.create(raw_metrics["train"])
        validation_eval = EvaluationScores.create(raw_metrics["validation"])
        return MetricScores(train_eval, validation_eval, raw_metrics["outerTrain"], raw_metrics["test"])

    def __str__(self) -> str:
        return f"(test={self.test}, outer_train={self.outer_train}, train={self.train}, validation={self.validation})"


class PipelineModel(Model, ABC):
    def best_parameters(self) -> "Series[Any]":
        best_params: Dict[str, Any] = self._list_info()["modelInfo"][0]["bestParameters"]
        return Series(best_params)

    def pipeline(self) -> "Series[Any]":
        steps: Dict[str, Any] = self._list_info()["modelInfo"][0]["pipeline"]
        return Series(steps)

    def metrics(self) -> "Series[Any]":
        model_metrics: Dict[str, Any] = self._list_info()["modelInfo"][0]["metrics"]
        metric_scores: Dict[str, MetricScores] = {k: MetricScores.create(v) for k, v in (model_metrics.items())}
        return Series(metric_scores)
