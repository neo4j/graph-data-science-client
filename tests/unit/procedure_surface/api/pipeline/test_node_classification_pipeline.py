from unittest import mock

from graphdatascience.procedure_surface.api.model.node_classification_model import NodeClassificationModel
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline import NodeClassificationPipeline
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline_results import (
    NodeClassificationPipelineInfoResult,
)
from graphdatascience.procedure_surface.api.pipeline.pipeline_endpoints import PipelineCatalogEntry


def test_node_classification_pipeline_exists_delegates_to_catalog_endpoint() -> None:
    ops = mock.Mock()
    trainer = mock.Mock()
    catalog = mock.Mock()
    catalog.exists.return_value = True

    pipeline = NodeClassificationPipeline("pipe", ops, trainer, catalog)

    assert pipeline.exists() is True
    catalog.exists.assert_called_once_with("pipe")


def test_node_classification_pipeline_details_delegates_to_catalog_endpoint() -> None:
    ops = mock.Mock()
    trainer = mock.Mock()
    catalog = mock.Mock()
    catalog.get.return_value = PipelineCatalogEntry(
        pipelineName="pipe",
        pipelineType="Node classification training pipeline",
        pipelineInfo={
            "featurePipeline": {
                "nodePropertySteps": [{"name": "gds.degree.mutate", "config": {"mutateProperty": "degree"}}],
                "featureProperties": [{"feature": "feature"}],
            },
            "splitConfig": {"testFraction": 0.2, "validationFolds": 2},
            "autoTuningConfig": {"maxTrials": 5},
            "trainingParameterSpace": {"LogisticRegression": [{"penalty": 0.0}]},
        },
    )

    pipeline = NodeClassificationPipeline("pipe", ops, trainer, catalog)

    result = pipeline.details()

    assert type(result) is NodeClassificationPipelineInfoResult
    assert result.name == "pipe"
    assert result.node_property_steps == [{"name": "gds.degree.mutate", "config": {"mutateProperty": "degree"}}]
    assert result.feature_properties == [{"feature": "feature"}]
    assert result.split_config == {"testFraction": 0.2, "validationFolds": 2}
    assert result.auto_tuning_config == {"maxTrials": 5}
    assert result.parameter_space == {"LogisticRegression": [{"penalty": 0.0}]}
    catalog.get.assert_called_once_with("pipe")


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
    model = NodeClassificationModel("model", mock.Mock(), predict)

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
