from abc import ABC
from dataclasses import dataclass
from typing import Any, Dict, List

from pandas.core.series import Series

from .model import Model


@dataclass(repr=True)
class EvaluationScores(Dict[str, float]):

    min: float
    avg: float
    max: float

    def __init__(self, min: float, avg: float, max: float):
        super(EvaluationScores, self).__init__({"min": min, "avg": avg, "max": max})
        self.__dict__ = self

    @staticmethod
    def create(raw_metrics: Dict[str, float]) -> "EvaluationScores":
        return EvaluationScores(raw_metrics["min"], raw_metrics["avg"], raw_metrics["max"])

    def __str__(self) -> str:
        return f"(min={self.min}, avg={self.avg}, max={self.max})"


@dataclass(repr=True)
class MetricScores(Dict[str, Any]):

    train: EvaluationScores
    validation: EvaluationScores
    outer_train: float
    test: float

    def __init__(self, train: EvaluationScores, validation: EvaluationScores, outer_train: float, test: float):
        super(MetricScores, self).__init__(
            {"train": train, "validation": validation, "outer_train": outer_train, "test": test}
        )
        self.__dict__ = self

    @staticmethod
    def create(raw_metrics: Dict[str, Any]) -> "MetricScores":
        train_eval = EvaluationScores.create(raw_metrics["train"])
        validation_eval = EvaluationScores.create(raw_metrics["validation"])
        return MetricScores(train_eval, validation_eval, raw_metrics["outerTrain"], raw_metrics["test"])

    def __str__(self) -> str:
        return f"(test={self.test}, outer_train={self.outer_train}, train={self.train}, validation={self.validation})"


@dataclass(frozen=True, repr=True)
class NodePropertyStep:

    proc: str
    config: Dict[str, Any]

    def __str__(self) -> str:
        return f"({self.proc}, {self.config})"


class PipelineModel(Model, ABC):
    def best_parameters(self) -> "Series[Any]":
        best_params: Dict[str, Any] = self._list_info()["modelInfo"][0]["bestParameters"]
        return Series(best_params)

    def node_property_steps(self) -> List[NodePropertyStep]:
        steps: List[Dict[str, Any]] = self._list_info()["modelInfo"][0]["pipeline"]["nodePropertySteps"]
        return [NodePropertyStep(s["name"], s["config"]) for s in steps]

    def metrics(self) -> "Series[Any]":
        model_metrics: Dict[str, Any] = self._list_info()["modelInfo"][0]["metrics"]
        metric_scores: Dict[str, MetricScores] = {k: MetricScores.create(v) for k, v in (model_metrics.items())}
        return Series(metric_scores)
