from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Any

from pandas.core.series import Series

from .model import Model


@dataclass(repr=True)
class EvaluationScores(dict[str, float]):
    min: float
    avg: float
    max: float

    def __init__(self, min: float, avg: float, max: float):
        super(EvaluationScores, self).__init__({"min": min, "avg": avg, "max": max})
        self.__dict__ = self

    @staticmethod
    def create(raw_metrics: dict[str, float]) -> "EvaluationScores":
        return EvaluationScores(raw_metrics["min"], raw_metrics["avg"], raw_metrics["max"])

    def __str__(self) -> str:
        return f"(min={self.min}, avg={self.avg}, max={self.max})"


@dataclass(repr=True)
class MetricScores(dict[str, Any]):
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
    def create(raw_metrics: dict[str, Any]) -> "MetricScores":
        train_eval = EvaluationScores.create(raw_metrics["train"])
        validation_eval = EvaluationScores.create(raw_metrics["validation"])
        return MetricScores(train_eval, validation_eval, raw_metrics["outerTrain"], raw_metrics["test"])

    def __str__(self) -> str:
        return f"(test={self.test}, outer_train={self.outer_train}, train={self.train}, validation={self.validation})"


@dataclass(frozen=True, repr=True)
class NodePropertyStep:
    """
    A node property step of a pipeline.
    Retrieve this for a pipeline using a `node_property_steps()` method of a pipeline model.

    Attributes:
        proc: The procedure for the node property step.
        config: The configuration of the node property step.
    """

    proc: str
    config: dict[str, Any]

    def __str__(self) -> str:
        return f"({self.proc}, {self.config})"


class PipelineModel(Model, ABC):
    def best_parameters(self) -> Series[Any]:
        """
        Get the best parameters for the pipeline model.

        Returns:
            The best parameters for the pipeline model.

        """
        best_params: dict[str, Any] = self._list_info()["modelInfo"]["bestParameters"]
        return Series(best_params)  # type: ignore[no-any-return]

    def node_property_steps(self) -> list[NodePropertyStep]:
        """
        Get the node property steps for the pipeline model.

        Returns:
            The node property steps for the pipeline model.

        """
        steps: list[dict[str, Any]] = self._list_info()["modelInfo"]["pipeline"]["nodePropertySteps"]
        return [NodePropertyStep(s["name"], s["config"]) for s in steps]

    def metrics(self) -> Series[Any]:
        """
        Get the metrics for the pipeline model.

        Returns:
            The metrics for the pipeline model.

        """
        model_metrics: dict[str, Any] = self._list_info()["modelInfo"]["metrics"]
        metric_scores: dict[str, MetricScores] = {k: MetricScores.create(v) for k, v in (model_metrics.items())}
        return Series(metric_scores)
