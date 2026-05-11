from unittest import mock

from graphdatascience.procedure_surface.api.pipeline.node_regression_pipeline import NodeRegressionPipeline
from graphdatascience.procedure_surface.api.pipeline.pipeline_endpoints import (
    PipelineCatalogEntry,
    PipelineExistsResult,
)


def test_node_regression_pipeline_exists_delegates_to_catalog_endpoint() -> None:
    ops = mock.Mock()
    trainer = mock.Mock()
    catalog = mock.Mock()
    catalog.exists.return_value = PipelineExistsResult(
        pipelineName="pipe",
        pipelineType="Node regression training pipeline",
        exists=True,
    )

    pipeline = NodeRegressionPipeline("pipe", ops, trainer, catalog)

    assert pipeline.exists() is True
    catalog.exists.assert_called_once_with("pipe")


def test_node_regression_pipeline_drop_delegates_to_catalog_endpoint() -> None:
    ops = mock.Mock()
    trainer = mock.Mock()
    catalog = mock.Mock()
    catalog.drop.return_value = PipelineCatalogEntry(
        pipelineName="pipe",
        pipelineType="Node regression training pipeline",
    )

    pipeline = NodeRegressionPipeline("pipe", ops, trainer, catalog)

    result = pipeline.drop()

    assert result is not None
    assert result.pipeline_name == "pipe"
    catalog.drop.assert_called_once_with("pipe", fail_if_missing=False)
