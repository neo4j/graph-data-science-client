from __future__ import annotations

from typing import Any

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize_single
from graphdatascience.procedure_surface.api.pipeline.link_prediction_pipeline import (
    LinkPredictionPipeline,
)
from graphdatascience.procedure_surface.api.pipeline.link_prediction_pipeline_endpoints import (
    LinkPredictionPipelineEndpoints,
)
from graphdatascience.procedure_surface.api.pipeline.link_prediction_pipeline_results import (
    LinkPredictionPipelineInfoResult,
)
from graphdatascience.procedure_surface.api.pipeline.link_prediction_predict_endpoints import (
    LinkPredictionPipelinePredictEndpoints,
)
from graphdatascience.procedure_surface.api.pipeline.link_prediction_train_endpoints import (
    LinkPredictionPipelineTrainEndpoints,
)
from graphdatascience.procedure_surface.api.pipeline.parameter_space_config import convert_to_parameter_space_config
from graphdatascience.procedure_surface.api.pipeline.pipeline_catalog_protocol import PipelineCatalogProtocol
from graphdatascience.procedure_surface.arrow.model_api_arrow import ModelApiArrow
from graphdatascience.procedure_surface.arrow.pipeline.link_prediction_predict_arrow_endpoints import (
    LinkPredictionPredictArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pipeline.link_prediction_train_arrow_endpoints import (
    LinkPredictionTrainArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pipeline.pipeline_catalog_arrow_endpoints import (
    PipelineCatalogArrowEndpoints,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.protocol.write_protocols import WriteProtocol


class LinkPredictionPipelineArrowEndpoints(LinkPredictionPipelineEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None,
        show_progress: bool = True,
    ) -> None:
        self._arrow_client = arrow_client
        self._write_protocol = write_protocol
        self._show_progress = show_progress
        self._predict = LinkPredictionPredictArrowEndpoints(
            arrow_client,
            write_protocol,
            show_progress=show_progress,
        )
        self._pipeline_catalog: PipelineCatalogProtocol = PipelineCatalogArrowEndpoints(
            arrow_client,
            show_progress=show_progress,
        )
        self._model_api = ModelApiArrow(arrow_client)
        self._train = LinkPredictionTrainArrowEndpoints(
            arrow_client=arrow_client,
            model_api=self._model_api,
            predict_endpoints=self._predict,
            show_progress=show_progress,
        )

    @property
    def train(self) -> LinkPredictionPipelineTrainEndpoints:
        return self._train

    @property
    def predict(self) -> LinkPredictionPipelinePredictEndpoints:
        return self._predict

    def create(self, pipeline_name: str) -> tuple[LinkPredictionPipeline, LinkPredictionPipelineInfoResult]:
        result = self._call_action("", pipeline_name=pipeline_name)
        return (
            LinkPredictionPipeline(pipeline_name, self, self, self._pipeline_catalog),
            LinkPredictionPipelineInfoResult(**result),
        )

    def get(self, pipeline_name: str) -> LinkPredictionPipeline:
        pipeline_info = self._pipeline_catalog.exists(pipeline_name)
        if not pipeline_info:
            raise ValueError(f"No pipeline named '{pipeline_name}' exists")
        if pipeline_info.pipeline_type != "Link prediction training pipeline":
            raise ValueError(f"Pipeline '{pipeline_name}' is not a link prediction pipeline")
        return LinkPredictionPipeline(
            pipeline_info.pipeline_name,
            self,
            self,
            self._pipeline_catalog,
        )

    def add_node_property(self, pipeline_name: str, task_name: str, **config: Any) -> LinkPredictionPipelineInfoResult:
        result = deserialize_single(
            self._arrow_client.do_action_with_retry(
                "v2/pipeline.linkPrediction.nodeProperty.add",
                {
                    "pipelineName": pipeline_name,
                    "procedureName": task_name,
                    "procedureConfiguration": ConfigConverter.convert_to_gds_config(**config),
                },
            )
        )
        return LinkPredictionPipelineInfoResult(**result)

    def add_feature(
        self,
        pipeline_name: str,
        feature_type: str,
        *,
        node_properties: list[str],
    ) -> LinkPredictionPipelineInfoResult:
        result = self._call_action(
            "feature.add",
            pipeline_name=pipeline_name,
            feature_type=feature_type,
            node_properties=node_properties,
        )
        return LinkPredictionPipelineInfoResult(**result)

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
    ) -> LinkPredictionPipelineInfoResult:
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
        return LinkPredictionPipelineInfoResult(**result)

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
    ) -> LinkPredictionPipelineInfoResult:
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
        return LinkPredictionPipelineInfoResult(**result)

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
    ) -> LinkPredictionPipelineInfoResult:
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
        return LinkPredictionPipelineInfoResult(**result)

    def configure_split(
        self,
        pipeline_name: str,
        *,
        negative_relationship_type: str | None = None,
        negative_sampling_ratio: float = 1.0,
        test_fraction: float = 0.1,
        train_fraction: float = 0.1,
        validation_folds: int = 3,
    ) -> LinkPredictionPipelineInfoResult:
        result = self._call_action(
            "split.configure",
            pipeline_name=pipeline_name,
            negative_relationship_type=negative_relationship_type,
            negative_sampling_ratio=negative_sampling_ratio,
            test_fraction=test_fraction,
            train_fraction=train_fraction,
            validation_folds=validation_folds,
        )
        return LinkPredictionPipelineInfoResult(**result)

    def configure_auto_tuning(self, pipeline_name: str, *, max_trials: int = 10) -> LinkPredictionPipelineInfoResult:
        result = self._call_action("autoTuning.configure", pipeline_name=pipeline_name, max_trials=max_trials)
        return LinkPredictionPipelineInfoResult(**result)

    def _call_action(self, suffix: str, **payload: Any) -> dict[str, Any]:
        config = ConfigConverter.convert_to_gds_config(**payload)
        endpoint = "v2/pipeline.linkPrediction" if not suffix else f"v2/pipeline.linkPrediction.{suffix}"
        return deserialize_single(self._arrow_client.do_action_with_retry(endpoint, config))
