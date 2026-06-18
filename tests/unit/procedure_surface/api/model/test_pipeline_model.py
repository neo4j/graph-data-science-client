import datetime
from unittest import mock

import pytest

from graphdatascience.model.model_details import ModelDetails
from graphdatascience.procedure_surface.api.model.link_prediction_model import LinkPredictionModel
from graphdatascience.procedure_surface.api.model.node_classification_model import NodeClassificationModel
from graphdatascience.procedure_surface.api.model.node_regression_model import NodeRegressionModel
from graphdatascience.procedure_surface.api.model.pipeline_model import PipelineModel


def _model_details() -> ModelDetails:
    return ModelDetails(
        modelName="model",
        modelType="NodeClassification",
        trainConfig={},
        graphSchema={},
        loaded=True,
        stored=False,
        published=False,
        creationTime=datetime.datetime(2026, 1, 1),
        modelInfo={
            "metrics": {"F1_MACRO": {"test": 0.9, "outerTrain": 0.95}},
            "bestParameters": {"penalty": 0.0, "maxEpochs": 200},
            "pipeline": {"nodePropertySteps": [{"name": "gds.hashgnn.mutate", "config": {"foo": "bar"}}]},
        },
    )


def _model(model_class: type[PipelineModel]) -> PipelineModel:
    model_api = mock.Mock()
    model_api.get.return_value = _model_details()
    return model_class("model", model_api)


@pytest.mark.parametrize("model_class", [NodeClassificationModel, NodeRegressionModel, LinkPredictionModel])
def test_pipeline_model_metrics(model_class: type[PipelineModel]) -> None:
    model = _model(model_class)

    assert model.metrics() == {"F1_MACRO": {"test": 0.9, "outerTrain": 0.95}}
    assert model.metrics()["F1_MACRO"] == {"test": 0.9, "outerTrain": 0.95}


@pytest.mark.parametrize("model_class", [NodeClassificationModel, NodeRegressionModel, LinkPredictionModel])
def test_pipeline_model_best_parameters(model_class: type[PipelineModel]) -> None:
    model = _model(model_class)

    assert model.best_parameters() == {"penalty": 0.0, "maxEpochs": 200}


@pytest.mark.parametrize("model_class", [NodeClassificationModel, NodeRegressionModel, LinkPredictionModel])
def test_pipeline_model_node_property_steps(model_class: type[PipelineModel]) -> None:
    model = _model(model_class)

    assert model.node_property_steps() == [{"name": "gds.hashgnn.mutate", "config": {"foo": "bar"}}]
