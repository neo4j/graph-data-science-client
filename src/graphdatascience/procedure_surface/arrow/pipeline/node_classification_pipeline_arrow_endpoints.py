from __future__ import annotations

from typing import Any

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize_single
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
from graphdatascience.procedure_surface.arrow.model_api_arrow import ModelApiArrow
from graphdatascience.procedure_surface.arrow.pipeline.node_classification_predict_arrow_endpoints import (
    NodeClassificationPredictArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pipeline.node_classification_train_arrow_endpoints import (
    NodeClassificationTrainArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pipeline.pipeline_catalog_arrow_endpoints import (
    PipelineCatalogArrowEndpoints,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.protocol.write_protocols import WriteProtocol


class NodeClassificationPipelineArrowEndpoints(NodeClassificationPipelineEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None,
        show_progress: bool = True,
    ) -> None:
        self._arrow_client = arrow_client
        self._write_protocol = write_protocol
        self._show_progress = show_progress
        self._predict = NodeClassificationPredictArrowEndpoints(
            arrow_client,
            write_protocol,
            show_progress=show_progress,
        )
        self._pipeline_catalog: PipelineCatalogProtocol = PipelineCatalogArrowEndpoints(
            arrow_client,
            show_progress=show_progress,
        )
        self._model_api = ModelApiArrow(arrow_client)
        self._train = NodeClassificationTrainArrowEndpoints(
            arrow_client=arrow_client,
            model_api=self._model_api,
            predict_endpoints=self._predict,
            show_progress=show_progress,
        )

    @property
    def train(self) -> NodeClassificationPipelineTrainEndpoints:
        return self._train

    @property
    def predict(self) -> NodeClassificationPipelinePredictEndpoints:
        return self._predict

    def create(self, pipeline_name: str) -> tuple[NodeClassificationPipeline, NodeClassificationPipelineInfoResult]:
        result = self._call_action("", pipeline_name=pipeline_name)
        return (
            NodeClassificationPipeline(pipeline_name, self, self, self._pipeline_catalog),
            NodeClassificationPipelineInfoResult(**result),
        )

    def get(self, pipeline_name: str) -> NodeClassificationPipeline:
        pipeline_info = self._pipeline_catalog.exists(pipeline_name)
        if not pipeline_info:
            raise ValueError(f"No pipeline named '{pipeline_name}' exists")
        if pipeline_info.pipeline_type != "Node classification training pipeline":
            raise ValueError(f"Pipeline '{pipeline_name}' is not a node classification pipeline")
        return NodeClassificationPipeline(
            pipeline_info.pipeline_name,
            self,
            self,
            self._pipeline_catalog,
        )

    def add_node_property(
        self, pipeline_name: str, task_name: str, **config: Any
    ) -> NodeClassificationPipelineInfoResult:
        result = deserialize_single(
            self._arrow_client.do_action_with_retry(
                "v2/pipeline.nodeClassification.nodeProperty.add",
                {
                    "pipelineName": pipeline_name,
                    "procedureName": task_name,
                    "procedureConfiguration": ConfigConverter.convert_to_gds_config(**config),
                },
            )
        )
        return NodeClassificationPipelineInfoResult(**result)

    def select_features(
        self, pipeline_name: str, node_properties: str | list[str]
    ) -> NodeClassificationPipelineInfoResult:
        result = self._call_action("features.select", pipeline_name=pipeline_name, node_properties=node_properties)
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
            model_type="MLP",
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

    def _call_action(self, suffix: str, **payload: Any) -> dict[str, Any]:
        config = ConfigConverter.convert_to_gds_config(**payload)
        endpoint = "v2/pipeline.nodeClassification" if not suffix else f"v2/pipeline.nodeClassification.{suffix}"
        return deserialize_single(self._arrow_client.do_action_with_retry(endpoint, config))
