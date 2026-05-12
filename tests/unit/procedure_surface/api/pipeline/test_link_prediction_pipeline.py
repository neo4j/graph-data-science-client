from typing import Any, cast
from unittest import mock

import pytest

from graphdatascience.procedure_surface.api.model.link_prediction_model import LinkPredictionModelV2
from graphdatascience.procedure_surface.api.pipeline import (
    LinkPredictionPipeline,
)
from graphdatascience.procedure_surface.api.pipeline.pipeline_endpoints import (
    PipelineCatalogEntry,
    PipelineExistsResult,
)


def test_link_prediction_pipeline_exists_delegates_to_catalog_endpoint() -> None:
    ops = mock.Mock()
    trainer = mock.Mock()
    catalog = mock.Mock()
    catalog.exists.return_value = PipelineExistsResult(
        pipelineName="pipe",
        pipelineType="Link prediction training pipeline",
        exists=True,
    )

    pipeline = LinkPredictionPipeline("pipe", ops, trainer, catalog)

    assert pipeline.exists() is True
    catalog.exists.assert_called_once_with("pipe")


def test_link_prediction_pipeline_drop_delegates_to_catalog_endpoint() -> None:
    ops = mock.Mock()
    trainer = mock.Mock()
    catalog = mock.Mock()
    catalog.drop.return_value = PipelineCatalogEntry(
        pipelineName="pipe",
        pipelineType="Link prediction training pipeline",
    )

    pipeline = LinkPredictionPipeline("pipe", ops, trainer, catalog)

    result = pipeline.drop()

    assert result is not None
    assert result.pipeline_name == "pipe"
    catalog.drop.assert_called_once_with("pipe", fail_if_missing=False)


def test_link_prediction_pipeline_delegates_add_node_property() -> None:
    ops = mock.Mock()
    trainer = mock.Mock()
    catalog = mock.Mock()
    pipeline = LinkPredictionPipeline("pipe", ops, trainer, catalog)

    pipeline.add_node_property("degree", mutate_property="score")

    ops.add_node_property.assert_called_once_with("pipe", "degree", mutate_property="score")


def test_link_prediction_pipeline_delegates_add_feature() -> None:
    ops = mock.Mock()
    trainer = mock.Mock()
    catalog = mock.Mock()
    pipeline = LinkPredictionPipeline("pipe", ops, trainer, catalog)

    pipeline.add_feature("l2", node_properties=["embedding"])

    ops.add_feature.assert_called_once_with("pipe", "l2", node_properties=["embedding"])


def test_link_prediction_pipeline_delegates_configure_auto_tuning() -> None:
    ops = mock.Mock()
    trainer = mock.Mock()
    catalog = mock.Mock()
    pipeline = LinkPredictionPipeline("pipe", ops, trainer, catalog)

    pipeline.configure_auto_tuning(max_trials=42)

    ops.configure_auto_tuning.assert_called_once_with("pipe", max_trials=42)


def test_link_prediction_model_predict_stream_rejects_target_relationship_type() -> None:
    model = LinkPredictionModelV2("model", mock.Mock(), mock.Mock())

    with pytest.raises(TypeError, match="target_relationship_type"):
        cast(Any, model.predict_stream)(mock.Mock(), target_relationship_type="REL")


def test_link_prediction_model_predict_estimate_rejects_target_relationship_type() -> None:
    model = LinkPredictionModelV2("model", mock.Mock(), mock.Mock())

    with pytest.raises(TypeError, match="target_relationship_type"):
        cast(Any, model.predict_estimate)(mock.Mock(), target_relationship_type="REL")


def test_link_prediction_model_predict_mutate_rejects_target_relationship_type() -> None:
    model = LinkPredictionModelV2("model", mock.Mock(), mock.Mock())

    with pytest.raises(TypeError, match="target_relationship_type"):
        cast(Any, model.predict_mutate)(mock.Mock(), "PREDICTED_REL", target_relationship_type="REL")


def test_link_prediction_model_predict_stream_delegates_top_n() -> None:
    predict = mock.Mock()
    model = LinkPredictionModelV2("model", mock.Mock(), predict)

    model.predict_stream(mock.Mock(), top_n=2)

    predict.stream.assert_called_once_with(
        mock.ANY,
        model_name="model",
        source_node_label=None,
        target_node_label=None,
        top_n=2,
        username=None,
        log_progress=True,
        sudo=False,
        concurrency=None,
        job_id=None,
    )


def test_link_prediction_model_predict_mutate_delegates_top_n() -> None:
    predict = mock.Mock()
    model = LinkPredictionModelV2("model", mock.Mock(), predict)

    model.predict_mutate(mock.Mock(), "PREDICTED_REL", top_n=2)

    predict.mutate.assert_called_once_with(
        mock.ANY,
        model_name="model",
        mutate_relationship_type="PREDICTED_REL",
        source_node_label=None,
        target_node_label=None,
        top_n=2,
        username=None,
        log_progress=True,
        sudo=False,
        concurrency=None,
        job_id=None,
    )
