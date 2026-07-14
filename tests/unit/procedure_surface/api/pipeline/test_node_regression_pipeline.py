from unittest import mock

from graphdatascience.procedure_surface.api.pipeline.node_regression_pipeline import NodeRegressionPipeline
from graphdatascience.procedure_surface.api.pipeline.node_regression_pipeline_results import (
    NodeRegressionPipelineInfoResult,
)
from graphdatascience.procedure_surface.api.pipeline.pipeline_endpoints import PipelineCatalogEntry


def test_node_regression_pipeline_exists_delegates_to_catalog_endpoint() -> None:
    ops = mock.Mock()
    trainer = mock.Mock()
    catalog = mock.Mock()
    catalog.exists.return_value = True

    pipeline = NodeRegressionPipeline("pipe", ops, trainer, catalog)

    assert pipeline.exists() is True
    catalog.exists.assert_called_once_with("pipe")


def test_node_regression_pipeline_details_delegates_to_catalog_endpoint() -> None:
    ops = mock.Mock()
    trainer = mock.Mock()
    catalog = mock.Mock()
    catalog.get.return_value = PipelineCatalogEntry(
        pipelineName="pipe",
        pipelineType="Node regression training pipeline",
        pipelineInfo={
            "featurePipeline": {
                "nodePropertySteps": [{"name": "gds.degree.mutate", "config": {"mutateProperty": "degree"}}],
                "featureProperties": [{"feature": "feature"}],
            },
            "splitConfig": {"testFraction": 0.2, "validationFolds": 2},
            "autoTuningConfig": {"maxTrials": 5},
            "trainingParameterSpace": {"LinearRegression": [{"penalty": 0.0}]},
        },
    )

    pipeline = NodeRegressionPipeline("pipe", ops, trainer, catalog)

    result = pipeline.details()

    assert type(result) is NodeRegressionPipelineInfoResult
    assert result.name == "pipe"
    assert result.node_property_steps == [{"name": "gds.degree.mutate", "config": {"mutateProperty": "degree"}}]
    assert result.feature_properties == [{"feature": "feature"}]
    assert result.split_config == {"testFraction": 0.2, "validationFolds": 2}
    assert result.auto_tuning_config == {"maxTrials": 5}
    assert result.parameter_space == {"LinearRegression": [{"penalty": 0.0}]}
    catalog.get.assert_called_once_with("pipe")


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
