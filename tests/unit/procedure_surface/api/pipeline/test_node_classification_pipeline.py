from unittest import mock

from graphdatascience.procedure_surface.api.model.node_classification_model import NodeClassificationModelV2
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

    result = pipeline.drop(fail_if_missing=True)

    assert result is not None
    assert result.pipeline_name == "pipe"
    catalog.drop.assert_called_once_with("pipe", fail_if_missing=True)


def test_node_classification_model_predict_stream_delegates_include_predicted_probabilities() -> None:
    predict = mock.Mock()
    model = NodeClassificationModelV2("model", mock.Mock(), predict)

    model.predict_stream(mock.Mock(), include_predicted_probabilities=False)

    predict.stream.assert_called_once_with(
        mock.ANY,
        model_name="model",
        relationship_types=None,
        target_node_labels=None,
        include_predicted_probabilities=False,
        username=None,
        log_progress=True,
        sudo=False,
        concurrency=None,
        job_id=None,
    )
