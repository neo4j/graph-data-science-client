from dataclasses import dataclass
from typing import Any

from .pipeline_model import PipelineModel


@dataclass(frozen=True, repr=True)
class LinkFeature:
    """
    A link feature of a link prediction pipeline.
    Retrieve this for a pipeline using
    :func:`LPModel.link_features() <graphdatascience.model.link_prediction_model.LPModel.link_features>`.

    Attributes:
        name: The name of the link feature.
        config: The configuration of the link feature.
    """

    name: str
    config: dict[str, Any]

    def __str__(self) -> str:
        return f"({self.name}, {self.config})"


class LPModel(PipelineModel):
    """
    Represents a link prediction model in the model catalog.
    Construct this using
    :func:`LPTrainingPipeline.train() <graphdatascience.pipeline.lp_training_pipeline.LPTrainingPipeline.train>`.
    """

    def _endpoint_prefix(self) -> str:
        return "gds.beta.pipeline.linkPrediction.predict."

    def link_features(self) -> list[LinkFeature]:
        """
        Get the link features of the pipeline.

        Returns:
            A list of LinkFeatures of the pipeline.

        """
        steps: list[dict[str, Any]] = self._list_info()["modelInfo"]["pipeline"]["featureSteps"]
        return [LinkFeature(s["name"], s["config"]) for s in steps]
