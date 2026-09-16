from __future__ import annotations

from typing import Any

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.model.node_classification_model import NodeClassificationModel
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline import (
    NodeClassificationPipeline,
)
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline_endpoints import (
    NodeClassificationPipelineEndpoints,
)
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline_results import (
    NodeClassificationPipelineInfoResult,
)
from graphdatascience.procedure_surface.api.pipeline.node_classification_predict_endpoints import (
    NodeClassificationPipelinePredictEndpoints,
)
from graphdatascience.procedure_surface.api.pipeline.node_classification_train_endpoints import (
    NodeClassificationPipelineTrainEndpoints,
)
from graphdatascience.procedure_surface.api.pipeline.parameter_space_config import convert_to_parameter_space_config
from graphdatascience.procedure_surface.api.pipeline.pipeline_catalog_protocol import PipelineCatalogProtocol
from graphdatascience.procedure_surface.cypher.model.model_catalog_cypher_endpoints import ModelCatalogCypherEndpoints
from graphdatascience.procedure_surface.cypher.pipeline.node_classification_predict_cypher_endpoints import (
    NodeClassificationPredictCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pipeline.node_classification_train_cypher_endpoints import (
    NodeClassificationTrainCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pipeline.pipeline_catalog_cypher_endpoints import (
    PipelineCatalogCypherEndpoints,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class NodeClassificationPipelineCypherEndpoints(NodeClassificationPipelineEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner
        self._pipeline_catalog: PipelineCatalogProtocol = PipelineCatalogCypherEndpoints(query_runner)
        self._model_catalog = ModelCatalogCypherEndpoints(query_runner)
        self._predict = NodeClassificationPredictCypherEndpoints(query_runner)
        self._train = NodeClassificationTrainCypherEndpoints(query_runner, self._predict)

    @property
    def train(self) -> NodeClassificationPipelineTrainEndpoints:
        return self._train

    @property
    def predict(self) -> NodeClassificationPipelinePredictEndpoints:
        return self._predict

    def create(self, pipeline_name: str) -> tuple[NodeClassificationPipeline, NodeClassificationPipelineInfoResult]:
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.nodeClassification.create", params=CallParameters(pipeline_name=pipeline_name)
        ).iloc[0]
        return NodeClassificationPipeline(
            pipeline_name, self, self, self._pipeline_catalog
        ), NodeClassificationPipelineInfoResult(**result)

    def get(self, pipeline_name: str) -> NodeClassificationPipeline:
        entry = self._pipeline_catalog.get(pipeline_name)
        if entry.pipeline_type != "Node classification training pipeline":
            raise ValueError(f"Pipeline '{pipeline_name}' is not a node classification pipeline")
        return NodeClassificationPipeline(
            entry.pipeline_name,
            self,
            self,
            self._pipeline_catalog,
        )

    def get_model(self, model_name: str) -> NodeClassificationModel:
        details = self._model_catalog.get(model_name)
        if details.model_type != "NodeClassification":
            raise ValueError(f"Model '{model_name}' is not a node classification model")
        return NodeClassificationModel(
            details.model_name,
            self._model_catalog,
            predict_endpoints=self._predict,
        )

    def add_node_property(
        self, pipeline_name: str, task_name: str, **config: Any
    ) -> NodeClassificationPipelineInfoResult:
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.nodeClassification.addNodeProperty",
            params=CallParameters(
                pipeline_name=pipeline_name,
                task_name=task_name,
                config=ConfigConverter.convert_to_gds_config(**config),
            ),
        ).iloc[0]
        return NodeClassificationPipelineInfoResult(**result)

    def select_features(
        self, pipeline_name: str, node_properties: str | list[str]
    ) -> NodeClassificationPipelineInfoResult:
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.nodeClassification.selectFeatures",
            params=CallParameters(pipeline_name=pipeline_name, node_properties=node_properties),
        ).iloc[0]
        return NodeClassificationPipelineInfoResult(**result)

    def add_logistic_regression(
        self,
        pipeline_name: str,
        *,
        batch_size: int | tuple[int, int] = 100,
        class_weights: list[float] | None = None,
        focus_weight: float | tuple[float, float] = 0.0,
        learning_rate: float | tuple[float, float] = 0.001,
        max_epochs: int | tuple[int, int] = 100,
        min_epochs: int | tuple[int, int] = 1,
        patience: int | tuple[int, int] = 1,
        penalty: float | tuple[float, float] = 0.0,
        tolerance: float | tuple[float, float] = 0.001,
    ) -> NodeClassificationPipelineInfoResult:
        config = convert_to_parameter_space_config(
            range_keys={
                "batch_size",
                "focus_weight",
                "learning_rate",
                "max_epochs",
                "min_epochs",
                "patience",
                "penalty",
                "tolerance",
            },
            batch_size=batch_size,
            class_weights=class_weights if class_weights is not None else [],
            focus_weight=focus_weight,
            learning_rate=learning_rate,
            max_epochs=max_epochs,
            min_epochs=min_epochs,
            patience=patience,
            penalty=penalty,
            tolerance=tolerance,
        )
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.nodeClassification.addLogisticRegression",
            params=CallParameters(pipeline_name=pipeline_name, config=config),
        ).iloc[0]
        return NodeClassificationPipelineInfoResult(**result)

    def add_random_forest(
        self,
        pipeline_name: str,
        *,
        criterion: str | None = "GINI",
        max_depth: int | tuple[int, int] = 2147483647,
        max_features_ratio: float | tuple[float, float] | None = None,
        min_leaf_size: int | tuple[int, int] = 1,
        min_split_size: int | tuple[int, int] = 2,
        number_of_decision_trees: int | tuple[int, int] = 100,
        number_of_samples_ratio: float | tuple[float, float] = 1.0,
    ) -> NodeClassificationPipelineInfoResult:
        config = convert_to_parameter_space_config(
            range_keys={
                "max_depth",
                "max_features_ratio",
                "min_leaf_size",
                "min_split_size",
                "number_of_decision_trees",
                "number_of_samples_ratio",
            },
            criterion=criterion,
            max_depth=max_depth,
            max_features_ratio=max_features_ratio,
            min_leaf_size=min_leaf_size,
            min_split_size=min_split_size,
            number_of_decision_trees=number_of_decision_trees,
            number_of_samples_ratio=number_of_samples_ratio,
        )
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.nodeClassification.addRandomForest",
            params=CallParameters(pipeline_name=pipeline_name, config=config),
        ).iloc[0]
        return NodeClassificationPipelineInfoResult(**result)

    def add_mlp(
        self,
        pipeline_name: str,
        *,
        batch_size: int | tuple[int, int] = 100,
        class_weights: list[float] | None = None,
        focus_weight: float | tuple[float, float] = 0.0,
        hidden_layer_sizes: list[int] = [100],
        learning_rate: float | tuple[float, float] = 0.001,
        max_epochs: int | tuple[int, int] = 100,
        min_epochs: int | tuple[int, int] = 1,
        patience: int | tuple[int, int] = 1,
        penalty: float | tuple[float, float] = 0.0,
        tolerance: float | tuple[float, float] = 0.001,
    ) -> NodeClassificationPipelineInfoResult:
        config = convert_to_parameter_space_config(
            range_keys={
                "batch_size",
                "focus_weight",
                "learning_rate",
                "max_epochs",
                "min_epochs",
                "patience",
                "penalty",
                "tolerance",
            },
            batch_size=batch_size,
            class_weights=class_weights if class_weights is not None else [],
            focus_weight=focus_weight,
            hidden_layer_sizes=hidden_layer_sizes,
            learning_rate=learning_rate,
            max_epochs=max_epochs,
            min_epochs=min_epochs,
            patience=patience,
            penalty=penalty,
            tolerance=tolerance,
        )
        result = self._query_runner.call_procedure(
            endpoint="gds.alpha.pipeline.nodeClassification.addMLP",
            params=CallParameters(pipeline_name=pipeline_name, config=config),
        ).iloc[0]
        return NodeClassificationPipelineInfoResult(**result)

    def configure_split(
        self, pipeline_name: str, *, test_fraction: float = 0.3, validation_folds: int = 3
    ) -> NodeClassificationPipelineInfoResult:
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.nodeClassification.configureSplit",
            params=CallParameters(
                pipeline_name=pipeline_name,
                config=ConfigConverter.convert_to_gds_config(
                    test_fraction=test_fraction, validation_folds=validation_folds
                ),
            ),
        ).iloc[0]
        return NodeClassificationPipelineInfoResult(**result)

    def configure_auto_tuning(
        self, pipeline_name: str, *, max_trials: int = 10
    ) -> NodeClassificationPipelineInfoResult:
        result = self._query_runner.call_procedure(
            endpoint="gds.alpha.pipeline.nodeClassification.configureAutoTuning",
            params=CallParameters(
                pipeline_name=pipeline_name,
                config=ConfigConverter.convert_to_gds_config(max_trials=max_trials),
            ),
        ).iloc[0]
        return NodeClassificationPipelineInfoResult(**result)
