from __future__ import annotations

from typing import Any

from graphdatascience.procedure_surface.api.pipeline.pipeline_catalog_result import PipelineCatalogEntry


def to_pipeline_info(entry: PipelineCatalogEntry, *, feature_key: str) -> dict[str, Any]:
    """Flatten a ``gds.pipeline.list`` catalog entry into the ``*PipelineInfoResult`` shape.

    The catalog's ``pipelineInfo`` nests the feature steps under ``featurePipeline`` and exposes the
    parameter space as ``trainingParameterSpace``, whereas the ``*PipelineInfoResult`` (as returned by
    the ``create``/``add``/``configure`` procedures) is flat and uses ``parameterSpace``. The pipeline
    name lives on the catalog entry itself rather than inside ``pipelineInfo``.

    Parameters
    ----------
    entry
        The catalog entry as returned by the pipeline catalog ``get``.
    feature_key
        The feature-step key inside ``featurePipeline`` for this pipeline type: ``featureProperties``
        for node classification/regression, ``featureSteps`` for link prediction.
    """
    info = entry.pipeline_info or {}
    feature_pipeline = info.get("featurePipeline", {})
    return {
        "name": entry.pipeline_name,
        "nodePropertySteps": feature_pipeline.get("nodePropertySteps", []),
        feature_key: feature_pipeline.get(feature_key, []),
        "splitConfig": info.get("splitConfig", {}),
        "autoTuningConfig": info.get("autoTuningConfig", {}),
        "parameterSpace": info.get("trainingParameterSpace", {}),
    }
