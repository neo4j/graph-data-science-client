from typing import Any

from .pipeline_model import PipelineModel


class NRModel(PipelineModel):
    """
    Represents a node regression model in the model catalog.
    Construct this using
    :func:`NRTrainingPipeline.train() <graphdatascience.pipeline.nr_training_pipeline.NRTrainingPipeline.train>`.
    """

    def _endpoint_prefix(self) -> str:
        return "gds.alpha.pipeline.nodeRegression.predict."

    def feature_properties(self) -> list[str]:
        """
        Get the feature properties of the model.

        Returns:
            The feature properties of the model.

        """
        features: list[dict[str, Any]] = self._list_info()["modelInfo"]["pipeline"]["featureProperties"]
        return [f["feature"] for f in features]
