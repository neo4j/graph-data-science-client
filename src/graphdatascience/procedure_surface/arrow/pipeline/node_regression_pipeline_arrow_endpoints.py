from __future__ import annotations

from typing import Any

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize_single
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.model.node_regression_model import NodeRegressionModelV2
from graphdatascience.procedure_surface.api.pipeline.node_regression_metric import NodeRegressionMetric
from graphdatascience.procedure_surface.api.pipeline.node_regression_pipeline import NodeRegressionPipeline
from graphdatascience.procedure_surface.api.pipeline.node_regression_pipeline_endpoints import (
    NodeRegressionPipelineEndpoints,
)
from graphdatascience.procedure_surface.api.pipeline.node_regression_pipeline_results import (
    NodeRegressionPipelineInfoResult,
    NodeRegressionPipelineTrainResult,
)
from graphdatascience.procedure_surface.api.pipeline.node_regression_predict_endpoints import (
    NodeRegressionPipelinePredictEndpoints,
)
from graphdatascience.procedure_surface.api.pipeline.parameter_space_config import convert_to_parameter_space_config
from graphdatascience.procedure_surface.api.pipeline.pipeline_catalog_protocol import PipelineCatalogProtocol
from graphdatascience.procedure_surface.arrow.model_api_arrow import ModelApiArrow
from graphdatascience.procedure_surface.arrow.pipeline.node_regression_predict_arrow_endpoints import (
    NodeRegressionPredictArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pipeline.pipeline_catalog_arrow_endpoints import (
    PipelineCatalogArrowEndpoints,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter


class NodeRegressionPipelineArrowEndpoints(NodeRegressionPipelineEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        show_progress: bool = True,
    ) -> None:
        self._arrow_client = arrow_client
        self._show_progress = show_progress
        self._predict = NodeRegressionPredictArrowEndpoints(
            arrow_client,
            None,
            show_progress=show_progress,
        )
        self._pipeline_catalog: PipelineCatalogProtocol = PipelineCatalogArrowEndpoints(
            arrow_client,
            show_progress=show_progress,
        )
        self._model_api = ModelApiArrow(arrow_client)

    @property
    def predict(self) -> NodeRegressionPipelinePredictEndpoints:
        return self._predict

    def create(self, pipeline_name: str) -> tuple[NodeRegressionPipeline, NodeRegressionPipelineInfoResult]:
        result = self._call_action("v2/pipeline.nodeRegression", pipeline_name=pipeline_name)
        return (
            NodeRegressionPipeline(pipeline_name, self, self, self._pipeline_catalog),
            NodeRegressionPipelineInfoResult(**result),
        )

    def get(self, pipeline_name: str) -> NodeRegressionPipeline:
        pipeline_info = self._pipeline_catalog.exists(pipeline_name)
        if not pipeline_info:
            raise ValueError(f"No pipeline named '{pipeline_name}' exists")
        if pipeline_info.pipeline_type != "Node regression training pipeline":
            raise ValueError(f"Pipeline '{pipeline_name}' is not a node regression pipeline")
        return NodeRegressionPipeline(
            pipeline_info.pipeline_name,
            self,
            self,
            self._pipeline_catalog,
        )

    def add_node_property(self, pipeline_name: str, task_name: str, **config: Any) -> NodeRegressionPipelineInfoResult:
        result = deserialize_single(
            self._arrow_client.do_action_with_retry(
                "v2/pipeline.nodeRegression.nodeProperty.add",
                {
                    "pipelineName": pipeline_name,
                    "procedureName": task_name,
                    "procedureConfiguration": ConfigConverter.convert_to_gds_config(**config),
                },
            )
        )
        return NodeRegressionPipelineInfoResult(**result)

    def select_features(
        self, pipeline_name: str, feature_properties: str | list[str]
    ) -> NodeRegressionPipelineInfoResult:
        result = self._call_action(
            "v2/pipeline.nodeRegression.features.select",
            pipeline_name=pipeline_name,
            feature_properties=feature_properties,
        )
        return NodeRegressionPipelineInfoResult(**result)

    def add_linear_regression(
        self,
        pipeline_name: str,
        *,
        batch_size: int | tuple[int, int] = 100,
        learning_rate: float | tuple[float, float] = 0.001,
        max_epochs: int | tuple[int, int] = 100,
        min_epochs: int | tuple[int, int] = 1,
        patience: int | tuple[int, int] = 1,
        penalty: float | tuple[float, float] = 0.0,
        tolerance: float | tuple[float, float] = 0.001,
    ) -> NodeRegressionPipelineInfoResult:
        config = convert_to_parameter_space_config(
            range_keys={"batch_size", "learning_rate", "max_epochs", "min_epochs", "patience", "penalty", "tolerance"},
            model_type="LinearRegression",
            batch_size=batch_size,
            learning_rate=learning_rate,
            max_epochs=max_epochs,
            min_epochs=min_epochs,
            patience=patience,
            penalty=penalty,
            tolerance=tolerance,
        )
        result = self._call_action(
            "v2/pipeline.nodeRegression.modelCandidate.add", pipeline_name=pipeline_name, **config
        )
        return NodeRegressionPipelineInfoResult(**result)

    def add_random_forest(
        self,
        pipeline_name: str,
        *,
        max_depth: int | tuple[int, int] = 2147483647,
        max_features_ratio: float | tuple[float, float] | None = None,
        min_leaf_size: int | tuple[int, int] = 1,
        min_split_size: int | tuple[int, int] = 2,
        number_of_decision_trees: int | tuple[int, int] = 100,
        number_of_samples_ratio: float | tuple[float, float] = 1.0,
    ) -> NodeRegressionPipelineInfoResult:
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
            max_depth=max_depth,
            max_features_ratio=max_features_ratio,
            min_leaf_size=min_leaf_size,
            min_split_size=min_split_size,
            number_of_decision_trees=number_of_decision_trees,
            number_of_samples_ratio=number_of_samples_ratio,
        )
        result = self._call_action(
            "v2/pipeline.nodeRegression.modelCandidate.add", pipeline_name=pipeline_name, **config
        )
        return NodeRegressionPipelineInfoResult(**result)

    def configure_split(
        self, pipeline_name: str, *, test_fraction: float = 0.3, validation_folds: int = 3
    ) -> NodeRegressionPipelineInfoResult:
        result = self._call_action(
            "v2/pipeline.nodeRegression.split.configure",
            pipeline_name=pipeline_name,
            test_fraction=test_fraction,
            validation_folds=validation_folds,
        )
        return NodeRegressionPipelineInfoResult(**result)

    def configure_auto_tuning(self, pipeline_name: str, *, max_trials: int = 10) -> NodeRegressionPipelineInfoResult:
        result = self._call_action(
            "v2/pipeline.nodeRegression.autoTuning.configure", pipeline_name=pipeline_name, max_trials=max_trials
        )
        return NodeRegressionPipelineInfoResult(**result)

    def train(
        self,
        G: Graph,
        pipeline_name: str,
        *,
        metrics: list[str | NodeRegressionMetric],
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
    ) -> tuple[NodeRegressionModelV2, NodeRegressionPipelineTrainResult]:
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            metrics=[metric.value if isinstance(metric, NodeRegressionMetric) else metric for metric in metrics],
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
            "v2/pipeline.nodeRegression.train",
            config,
            show_progress=show_progress,
        )
        result = JobClient.get_summary(self._arrow_client, result_job_id)
        return (
            NodeRegressionModelV2(
                model_name,
                self._model_api,
                predict_endpoints=self._predict,
            ),
            NodeRegressionPipelineTrainResult(**result),
        )

    def _call_action(self, endpoint: str, **payload: Any) -> dict[str, Any]:
        config = ConfigConverter.convert_to_gds_config(**payload)
        return deserialize_single(self._arrow_client.do_action_with_retry(endpoint, config))
