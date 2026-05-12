from __future__ import annotations

from typing import Any

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.pipeline.link_prediction_pipeline import LinkPredictionPipeline
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
from graphdatascience.procedure_surface.cypher.pipeline.link_prediction_predict_cypher_endpoints import (
    LinkPredictionPredictCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pipeline.link_prediction_train_cypher_endpoints import (
    LinkPredictionTrainCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pipeline.pipeline_catalog_cypher_endpoints import (
    PipelineCatalogCypherEndpoints,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class LinkPredictionPipelineCypherEndpoints(LinkPredictionPipelineEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner
        self._pipeline_catalog: PipelineCatalogProtocol = PipelineCatalogCypherEndpoints(query_runner)
        self._predict = LinkPredictionPredictCypherEndpoints(query_runner)
        self._train = LinkPredictionTrainCypherEndpoints(query_runner, self._predict)

    @property
    def train(self) -> LinkPredictionPipelineTrainEndpoints:
        return self._train

    @property
    def predict(self) -> LinkPredictionPipelinePredictEndpoints:
        return self._predict

    def create(self, pipeline_name: str) -> tuple[LinkPredictionPipeline, LinkPredictionPipelineInfoResult]:
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.linkPrediction.create",
            params=CallParameters(pipeline_name=pipeline_name),
        ).squeeze()
        return (
            LinkPredictionPipeline(pipeline_name, self, self, self._pipeline_catalog),
            LinkPredictionPipelineInfoResult(**result.to_dict()),
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
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.linkPrediction.addNodeProperty",
            params=CallParameters(
                pipeline_name=pipeline_name,
                task_name=task_name,
                config=ConfigConverter.convert_to_gds_config(**config),
            ),
        ).squeeze()
        return LinkPredictionPipelineInfoResult(**result.to_dict())

    def add_feature(
        self,
        pipeline_name: str,
        feature_type: str,
        *,
        node_properties: list[str],
    ) -> LinkPredictionPipelineInfoResult:
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.linkPrediction.addFeature",
            params=CallParameters(
                pipeline_name=pipeline_name,
                feature_type=feature_type,
                config=ConfigConverter.convert_to_gds_config(node_properties=node_properties),
            ),
        ).squeeze()
        return LinkPredictionPipelineInfoResult(**result.to_dict())

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
            endpoint="gds.beta.pipeline.linkPrediction.addLogisticRegression",
            params=CallParameters(pipeline_name=pipeline_name, config=config),
        ).squeeze()
        return LinkPredictionPipelineInfoResult(**result.to_dict())

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
            criterion=criterion,
            max_depth=max_depth,
            max_features_ratio=max_features_ratio,
            min_leaf_size=min_leaf_size,
            min_split_size=min_split_size,
            number_of_decision_trees=number_of_decision_trees,
            number_of_samples_ratio=number_of_samples_ratio,
        )
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.linkPrediction.addRandomForest",
            params=CallParameters(pipeline_name=pipeline_name, config=config),
        ).squeeze()
        return LinkPredictionPipelineInfoResult(**result.to_dict())

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
            endpoint="gds.alpha.pipeline.linkPrediction.addMLP",
            params=CallParameters(pipeline_name=pipeline_name, config=config),
        ).squeeze()
        return LinkPredictionPipelineInfoResult(**result.to_dict())

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
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.linkPrediction.configureSplit",
            params=CallParameters(
                pipeline_name=pipeline_name,
                config=ConfigConverter.convert_to_gds_config(
                    negative_relationship_type=negative_relationship_type,
                    negative_sampling_ratio=negative_sampling_ratio,
                    test_fraction=test_fraction,
                    train_fraction=train_fraction,
                    validation_folds=validation_folds,
                ),
            ),
        ).squeeze()
        return LinkPredictionPipelineInfoResult(**result.to_dict())

    def configure_auto_tuning(self, pipeline_name: str, *, max_trials: int = 10) -> LinkPredictionPipelineInfoResult:
        result = self._query_runner.call_procedure(
            endpoint="gds.alpha.pipeline.linkPrediction.configureAutoTuning",
            params=CallParameters(
                pipeline_name=pipeline_name,
                config=ConfigConverter.convert_to_gds_config(max_trials=max_trials),
            ),
        ).squeeze()
        return LinkPredictionPipelineInfoResult(**result.to_dict())
