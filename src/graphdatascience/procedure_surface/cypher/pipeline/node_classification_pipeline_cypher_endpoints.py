from __future__ import annotations

from typing import Any

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.model.node_classification_model import NodeClassificationModelV2
from graphdatascience.procedure_surface.api.node_classification_predict_endpoints import (
    NodeClassificationPipelinePredictEndpoints,
)
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline import (
    NodeClassificationPipeline,
)
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline_endpoints import (
    NodeClassificationPipelineEndpoints,
)
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline_results import (
    NodeClassificationPipelineInfoResult,
    NodeClassificationPipelineTrainResult,
)
from graphdatascience.procedure_surface.api.pipeline.parameter_space_config import convert_to_parameter_space_config
from graphdatascience.procedure_surface.cypher.model_api_cypher import ModelApiCypher
from graphdatascience.procedure_surface.cypher.pipeline.node_classification_predict_cypher_endpoints import (
    NodeClassificationPredictCypherEndpoints,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class NodeClassificationPipelineCypherEndpoints(NodeClassificationPipelineEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner
        self._predict = NodeClassificationPredictCypherEndpoints(query_runner)

    @property
    def predict(self) -> NodeClassificationPipelinePredictEndpoints:
        return self._predict

    def create(self, pipeline_name: str) -> tuple[NodeClassificationPipeline, NodeClassificationPipelineInfoResult]:
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.nodeClassification.create", params=CallParameters(pipeline_name=pipeline_name)
        ).squeeze()
        return NodeClassificationPipeline(pipeline_name, self, self), NodeClassificationPipelineInfoResult(
            **result.to_dict()
        )

    def get(self, pipeline_name: str) -> NodeClassificationPipeline:
        result = self._query_runner.call_procedure(
            "gds.pipeline.list",
            params=CallParameters(pipeline_name=pipeline_name),
            custom_error=False,
        )
        if result.empty:
            raise ValueError(f"No pipeline named '{pipeline_name}' exists")
        pipeline_info = result.iloc[0].to_dict()
        if pipeline_info.get("pipelineType") != "Node classification training pipeline":
            raise ValueError(f"Pipeline '{pipeline_name}' is not a node classification pipeline")
        return NodeClassificationPipeline(str(pipeline_info.get("pipelineName", pipeline_name)), self, self)

    def add_node_property(
        self, pipeline_name: str, procedure_name: str, **config: Any
    ) -> NodeClassificationPipelineInfoResult:
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.nodeClassification.addNodeProperty",
            params=CallParameters(
                pipeline_name=pipeline_name,
                procedure_name=procedure_name,
                config=ConfigConverter.convert_to_gds_config(**config),
            ),
        ).squeeze()
        return NodeClassificationPipelineInfoResult(**result.to_dict())

    def select_features(
        self, pipeline_name: str, feature_properties: str | list[str]
    ) -> NodeClassificationPipelineInfoResult:
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.nodeClassification.selectFeatures",
            params=CallParameters(pipeline_name=pipeline_name, feature_properties=feature_properties),
        ).squeeze()
        return NodeClassificationPipelineInfoResult(**result.to_dict())

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
        ).squeeze()
        return NodeClassificationPipelineInfoResult(**result.to_dict())

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
        ).squeeze()
        return NodeClassificationPipelineInfoResult(**result.to_dict())

    def add_mlp(
        self,
        pipeline_name: str,
        *,
        hidden_layer_sizes: list[int],
        penalty: float | tuple[float, float] = 0.0,
    ) -> NodeClassificationPipelineInfoResult:
        config = convert_to_parameter_space_config(
            range_keys={"penalty"},
            hidden_layer_sizes=hidden_layer_sizes,
            penalty=penalty,
        )
        result = self._query_runner.call_procedure(
            endpoint="gds.alpha.pipeline.nodeClassification.addMLP",
            params=CallParameters(pipeline_name=pipeline_name, config=config),
        ).squeeze()
        return NodeClassificationPipelineInfoResult(**result.to_dict())

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
        ).squeeze()
        return NodeClassificationPipelineInfoResult(**result.to_dict())

    def configure_auto_tuning(
        self, pipeline_name: str, *, max_trials: int = 10
    ) -> NodeClassificationPipelineInfoResult:
        result = self._query_runner.call_procedure(
            endpoint="gds.alpha.pipeline.nodeClassification.configureAutoTuning",
            params=CallParameters(
                pipeline_name=pipeline_name,
                config=ConfigConverter.convert_to_gds_config(max_trials=max_trials),
            ),
        ).squeeze()
        return NodeClassificationPipelineInfoResult(**result.to_dict())

    def train(
        self,
        G: GraphV2,
        pipeline_name: str,
        *,
        metrics: list[str],
        model_name: str,
        target_property: str,
        relationship_types: list[str] = ALL_TYPES,
        target_node_labels: list[str] = ALL_LABELS,
        store_model_to_disk: bool = False,
        random_seed: Any | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> tuple[NodeClassificationModelV2, NodeClassificationPipelineTrainResult]:
        gds_config = ConfigConverter.convert_to_gds_config(
            metrics=metrics,
            model_name=model_name,
            pipeline=pipeline_name,
            target_property=target_property,
            relationship_types=relationship_types,
            target_node_labels=target_node_labels,
            store_model_to_disk=store_model_to_disk,
            random_seed=random_seed,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )
        params = CallParameters(graph_name=G.name(), config=gds_config)
        params.ensure_job_id_in_config()
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.nodeClassification.train", params=params, logging=True
        ).squeeze()
        return (
            NodeClassificationModelV2(
                name=model_name,
                model_api=ModelApiCypher(self._query_runner),
                predict_endpoints=self._predict,
            ),
            NodeClassificationPipelineTrainResult(**result.to_dict()),
        )
