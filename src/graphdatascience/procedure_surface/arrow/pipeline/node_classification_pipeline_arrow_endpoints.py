from __future__ import annotations

from typing import Any

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize, deserialize_single
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
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
from graphdatascience.procedure_surface.arrow.model_api_arrow import ModelApiArrow
from graphdatascience.procedure_surface.arrow.pipeline.node_classification_predict_arrow_endpoints import (
    NodeClassificationPredictArrowEndpoints,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter


class NodeClassificationPipelineArrowEndpoints(NodeClassificationPipelineEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None,
        show_progress: bool = True,
    ) -> None:
        self._arrow_client = arrow_client
        self._show_progress = show_progress
        self._predict = NodeClassificationPredictArrowEndpoints(
            arrow_client,
            write_back_client,
            show_progress=show_progress,
        )
        self._model_api = ModelApiArrow(arrow_client)

    @property
    def predict(self) -> NodeClassificationPipelinePredictEndpoints:
        return self._predict

    def create(self, pipeline_name: str) -> tuple[NodeClassificationPipeline, NodeClassificationPipelineInfoResult]:
        result = self._call_action("", pipeline_name=pipeline_name)
        return NodeClassificationPipeline(pipeline_name, self, self), NodeClassificationPipelineInfoResult(**result)

    def get(self, pipeline_name: str) -> NodeClassificationPipeline:
        result = deserialize(
            self._arrow_client.do_action_with_retry(
                "v2/pipeline.list",
                ConfigConverter.convert_to_gds_config(pipeline_name=pipeline_name),
            )
        )
        if not result:
            raise ValueError(f"No pipeline named '{pipeline_name}' exists")
        if len(result) != 1:
            raise ValueError(f"Expected exactly one pipeline named '{pipeline_name}', got {len(result)}")
        pipeline_info = result[0]
        if pipeline_info.get("pipelineType") != "Node classification training pipeline":
            raise ValueError(f"Pipeline '{pipeline_name}' is not a node classification pipeline")
        return NodeClassificationPipeline(pipeline_info.get("pipelineName", pipeline_name), self, self)

    def add_node_property(
        self, pipeline_name: str, procedure_name: str, **config: Any
    ) -> NodeClassificationPipelineInfoResult:
        result = deserialize_single(
            self._arrow_client.do_action_with_retry(
                "v2/pipeline.nodeClassification.nodeProperty.add",
                {
                    "pipelineName": pipeline_name,
                    "procedureName": procedure_name,
                    "procedureConfiguration": ConfigConverter.convert_to_gds_config(**config),
                },
            )
        )
        return NodeClassificationPipelineInfoResult(**result)

    def select_features(
        self, pipeline_name: str, feature_properties: str | list[str]
    ) -> NodeClassificationPipelineInfoResult:
        result = self._call_action(
            "features.select", pipeline_name=pipeline_name, feature_properties=feature_properties
        )
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
            model_type="LogisticRegression",
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
        result = self._call_action("modelCandidate.add", pipeline_name=pipeline_name, **config)
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
            model_type="RandomForest",
            criterion=criterion,
            max_depth=max_depth,
            max_features_ratio=max_features_ratio,
            min_leaf_size=min_leaf_size,
            min_split_size=min_split_size,
            number_of_decision_trees=number_of_decision_trees,
            number_of_samples_ratio=number_of_samples_ratio,
        )
        result = self._call_action("modelCandidate.add", pipeline_name=pipeline_name, **config)
        return NodeClassificationPipelineInfoResult(**result)

    def add_mlp(
        self,
        pipeline_name: str,
        *,
        hidden_layer_sizes: list[int],
        penalty: float | tuple[float, float] = 0.0,
    ) -> NodeClassificationPipelineInfoResult:
        config = convert_to_parameter_space_config(
            range_keys={"penalty"},
            model_type="MLP",
            hidden_layer_sizes=hidden_layer_sizes,
            penalty=penalty,
        )
        result = self._call_action("modelCandidate.add", pipeline_name=pipeline_name, **config)
        return NodeClassificationPipelineInfoResult(**result)

    def configure_split(
        self, pipeline_name: str, *, test_fraction: float = 0.3, validation_folds: int = 3
    ) -> NodeClassificationPipelineInfoResult:
        result = self._call_action(
            "split.configure",
            pipeline_name=pipeline_name,
            test_fraction=test_fraction,
            validation_folds=validation_folds,
        )
        return NodeClassificationPipelineInfoResult(**result)

    def configure_auto_tuning(
        self, pipeline_name: str, *, max_trials: int = 10
    ) -> NodeClassificationPipelineInfoResult:
        result = self._call_action("autoTuning.configure", pipeline_name=pipeline_name, max_trials=max_trials)
        return NodeClassificationPipelineInfoResult(**result)

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
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
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
        show_progress = self._show_progress and log_progress
        result_job_id = JobClient.run_job_and_wait(
            self._arrow_client,
            "v2/pipeline.nodeClassification.train",
            config,
            show_progress=show_progress,
        )
        result = JobClient.get_summary(self._arrow_client, result_job_id)
        return (
            NodeClassificationModelV2(
                model_name,
                self._model_api,
                predict_endpoints=self._predict,
            ),
            NodeClassificationPipelineTrainResult(**result),
        )

    def _call_action(self, suffix: str, **payload: Any) -> dict[str, Any]:
        config = ConfigConverter.convert_to_gds_config(**payload)
        endpoint = "v2/pipeline.nodeClassification" if not suffix else f"v2/pipeline.nodeClassification.{suffix}"
        return deserialize_single(self._arrow_client.do_action_with_retry(endpoint, config))
