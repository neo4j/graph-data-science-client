from unittest import mock

from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline import NodeClassificationPipeline
from graphdatascience.procedure_surface.api.pipeline.pipeline_endpoints import (
    PipelineCatalogEntry,
    PipelineExistsResult,
)


def test_node_classification_pipeline_exists_delegates_to_catalog_endpoint() -> None:
    ops = mock.Mock()
    trainer = mock.Mock()
    catalog = mock.Mock()
    catalog.exists.return_value = PipelineExistsResult(
        pipelineName="pipe",
        pipelineType="Node classification training pipeline",
        exists=True,
    )

    pipeline = NodeClassificationPipeline("pipe", ops, trainer, catalog)

    assert pipeline.exists() is True
    catalog.exists.assert_called_once_with("pipe")


def test_node_classification_pipeline_drop_delegates_to_catalog_endpoint() -> None:
    ops = mock.Mock()
    trainer = mock.Mock()
    catalog = mock.Mock()
    catalog.drop.return_value = PipelineCatalogEntry(
        pipelineName="pipe",
        pipelineType="Node classification training pipeline",
    )

    pipeline = NodeClassificationPipeline("pipe", ops, trainer, catalog)

    result = pipeline.drop(failIfMissing=True)

    assert result is not None
    assert result.pipeline_name == "pipe"
    catalog.drop.assert_called_once_with("pipe", fail_if_missing=True)
