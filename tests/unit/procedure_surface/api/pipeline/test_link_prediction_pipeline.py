from typing import Any, cast
from unittest import mock

import pytest

from graphdatascience.procedure_surface.api.model.link_prediction_model import LinkPredictionModel
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


def test_link_prediction_pipeline_delegates_configure_split() -> None:
    ops = mock.Mock()
    trainer = mock.Mock()
    catalog = mock.Mock()
    pipeline = LinkPredictionPipeline("pipe", ops, trainer, catalog)

    pipeline.configure_split(
        negative_relationship_type="NO_REL",
        negative_sampling_ratio=2.5,
        test_fraction=0.2,
        train_fraction=0.7,
        validation_folds=5,
    )

    ops.configure_split.assert_called_once_with(
        "pipe",
        negative_relationship_type="NO_REL",
        negative_sampling_ratio=2.5,
        test_fraction=0.2,
        train_fraction=0.7,
        validation_folds=5,
    )


def test_link_prediction_pipeline_delegates_add_random_forest() -> None:
    ops = mock.Mock()
    trainer = mock.Mock()
    catalog = mock.Mock()
    pipeline = LinkPredictionPipeline("pipe", ops, trainer, catalog)

    pipeline.add_random_forest(
        criterion="ENTROPY",
        max_depth=8,
        max_features_ratio=0.4,
        min_leaf_size=3,
        min_split_size=6,
        number_of_decision_trees=40,
        number_of_samples_ratio=0.9,
    )

    ops.add_random_forest.assert_called_once_with(
        "pipe",
        criterion="ENTROPY",
        max_depth=8,
        max_features_ratio=0.4,
        min_leaf_size=3,
        min_split_size=6,
        number_of_decision_trees=40,
        number_of_samples_ratio=0.9,
    )


def test_link_prediction_pipeline_delegates_configure_auto_tuning() -> None:
    ops = mock.Mock()
    trainer = mock.Mock()
    catalog = mock.Mock()
    pipeline = LinkPredictionPipeline("pipe", ops, trainer, catalog)

    pipeline.configure_auto_tuning(max_trials=42)

    ops.configure_auto_tuning.assert_called_once_with("pipe", max_trials=42)


def test_link_prediction_model_predict_stream_rejects_target_relationship_type() -> None:
    model = LinkPredictionModel("model", mock.Mock(), mock.Mock())

    with pytest.raises(TypeError, match="target_relationship_type"):
        cast(Any, model.predict_stream)(mock.Mock(), target_relationship_type="REL")


def test_link_prediction_model_predict_estimate_rejects_target_relationship_type() -> None:
    model = LinkPredictionModel("model", mock.Mock(), mock.Mock())

    with pytest.raises(TypeError, match="target_relationship_type"):
        cast(Any, model.predict_estimate)(mock.Mock(), target_relationship_type="REL")


def test_link_prediction_model_predict_mutate_rejects_target_relationship_type() -> None:
    model = LinkPredictionModel("model", mock.Mock(), mock.Mock())

    with pytest.raises(TypeError, match="target_relationship_type"):
        cast(Any, model.predict_mutate)(mock.Mock(), "PREDICTED_REL", target_relationship_type="REL")


def test_link_prediction_model_predict_stream_delegates_top_n() -> None:
    predict = mock.Mock()
    model = LinkPredictionModel("model", mock.Mock(), predict)

    model.predict_stream(
        mock.Mock(),
        relationship_types=["REL"],
        sample_rate=0.7,
        threshold=0.5,
        top_k=3,
        top_n=2,
        initial_sampler="UNIFORM",
        delta_threshold=0.01,
        max_iterations=10,
        random_joins=4,
        random_seed=42,
    )

    predict.stream.assert_called_once_with(
        mock.ANY,
        model_name="model",
        relationship_types=["REL"],
        sample_rate=0.7,
        threshold=0.5,
        top_k=3,
        source_node_label=None,
        target_node_label=None,
        top_n=2,
        initial_sampler="UNIFORM",
        delta_threshold=0.01,
        max_iterations=10,
        random_joins=4,
        random_seed=42,
        username=None,
        log_progress=True,
        sudo=False,
        concurrency=None,
        job_id=None,
    )


def test_link_prediction_model_predict_mutate_delegates_top_n() -> None:
    predict = mock.Mock()
    model = LinkPredictionModel("model", mock.Mock(), predict)

    model.predict_mutate(
        mock.Mock(),
        "PREDICTED_REL",
        mutate_property="score",
        relationship_types=["REL"],
        sample_rate=0.7,
        threshold=0.5,
        top_k=3,
        top_n=2,
        initial_sampler="UNIFORM",
        delta_threshold=0.01,
        max_iterations=10,
        random_joins=4,
        random_seed=42,
    )

    predict.mutate.assert_called_once_with(
        mock.ANY,
        model_name="model",
        mutate_relationship_type="PREDICTED_REL",
        mutate_property="score",
        relationship_types=["REL"],
        sample_rate=0.7,
        threshold=0.5,
        top_k=3,
        source_node_label=None,
        target_node_label=None,
        top_n=2,
        initial_sampler="UNIFORM",
        delta_threshold=0.01,
        max_iterations=10,
        random_joins=4,
        random_seed=42,
        username=None,
        log_progress=True,
        sudo=False,
        concurrency=None,
        job_id=None,
    )
