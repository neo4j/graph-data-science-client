from __future__ import annotations

from abc import ABC
from typing import Any

from graphdatascience.model.model import Model


class PipelineModel(Model, ABC):
    """
    Base class for models produced by training pipelines (node classification,
    node regression and link prediction).

    Exposes the training metadata that is stored alongside the model in the
    model catalog (the ``modelInfo`` of :meth:`details`).
    """

    def metrics(self) -> dict[str, Any]:
        """
        Get the metrics computed while training the model.

        Returns
        -------
        dict[str, Any]
            The metrics keyed by metric name (e.g. ``F1_MACRO``).
        """
        metrics: dict[str, Any] = self.details().model_info["metrics"]
        return metrics

    def best_parameters(self) -> dict[str, Any]:
        """
        Get the parameters of the winning model candidate selected during training.

        Returns
        -------
        dict[str, Any]
            The configuration of the best performing model candidate.
        """
        best_parameters: dict[str, Any] = self.details().model_info["bestParameters"]
        return best_parameters

    def node_property_steps(self) -> list[dict[str, Any]]:
        """
        Get the node property steps of the pipeline that produced the model.

        Returns
        -------
        list[dict[str, Any]]
            The configured node property steps.
        """
        steps: list[dict[str, Any]] = self.details().model_info["pipeline"]["nodePropertySteps"]
        return steps
