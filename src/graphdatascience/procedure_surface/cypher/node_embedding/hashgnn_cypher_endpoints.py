from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.node_embedding.hashgnn_endpoints import (
    HashGNNEndpoints,
    HashGNNMutateResult,
    HashGNNWriteResult,
)
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class HashGNNCypherEndpoints(HashGNNEndpoints):
    """
    Implementation of the HashGNN algorithm endpoints.
    This class handles the actual execution by forwarding calls to the query runner.
    """

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        iterations: int,
        embedding_density: int,
        mutate_property: str,
        output_dimension: int | None = None,
        neighbor_influence: float = 1.0,
        generate_features: dict[str, Any] | None = None,
        binarize_features: dict[str, Any] | None = None,
        heterogeneous: bool = False,
        feature_properties: list[str] | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> HashGNNMutateResult:
        """
        Compute node embeddings using HashGNN and store the result as a node property in the in-memory graph.
        """

        config = ConfigConverter.convert_to_gds_config(
            iterations=iterations,
            embedding_density=embedding_density,
            mutate_property=mutate_property,
            output_dimension=output_dimension,
            neighbor_influence=neighbor_influence,
            generate_features=generate_features,
            binarize_features=binarize_features,
            heterogeneous=heterogeneous,
            feature_properties=feature_properties,
            random_seed=random_seed,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.hashgnn.mutate", params=params, logging=log_progress
        ).squeeze()

        return HashGNNMutateResult(**result.to_dict())

    def stream(
        self,
        G: GraphV2,
        iterations: int,
        embedding_density: int,
        output_dimension: int | None = None,
        neighbor_influence: float = 1.0,
        generate_features: dict[str, Any] | None = None,
        binarize_features: dict[str, Any] | None = None,
        heterogeneous: bool = False,
        feature_properties: list[str] | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Compute node embeddings using HashGNN and stream the results.
        """

        config = ConfigConverter.convert_to_gds_config(
            iterations=iterations,
            embedding_density=embedding_density,
            output_dimension=output_dimension,
            neighbor_influence=neighbor_influence,
            generate_features=generate_features,
            binarize_features=binarize_features,
            heterogeneous=heterogeneous,
            feature_properties=feature_properties,
            random_seed=random_seed,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.hashgnn.stream", params=params, logging=log_progress)

    def write(
        self,
        G: GraphV2,
        iterations: int,
        embedding_density: int,
        write_property: str,
        output_dimension: int | None = None,
        neighbor_influence: float = 1.0,
        generate_features: dict[str, Any] | None = None,
        binarize_features: dict[str, Any] | None = None,
        heterogeneous: bool = False,
        feature_properties: list[str] | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
        random_seed: int | None = None,
    ) -> HashGNNWriteResult:
        """
        Compute node embeddings using HashGNN and write the results to the Neo4j database.
        """

        config = ConfigConverter.convert_to_gds_config(
            iterations=iterations,
            embedding_density=embedding_density,
            write_property=write_property,
            output_dimension=output_dimension,
            neighbor_influence=neighbor_influence,
            generate_features=generate_features,
            binarize_features=binarize_features,
            heterogeneous=heterogeneous,
            feature_properties=feature_properties,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
            write_concurrency=write_concurrency,
            random_seed=random_seed,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.hashgnn.write", params=params, logging=log_progress
        ).squeeze()

        return HashGNNWriteResult(**result.to_dict())

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        iterations: int,
        embedding_density: int,
        output_dimension: int | None = None,
        neighbor_influence: float = 1.0,
        generate_features: dict[str, Any] | None = None,
        binarize_features: dict[str, Any] | None = None,
        heterogeneous: bool = False,
        feature_properties: list[str] | None = None,
        random_seed: int | None = None,
    ) -> EstimationResult:
        """
        Estimate the cost of running HashGNN on the given graph.
        """

        config = ConfigConverter.convert_to_gds_config(
            iterations=iterations,
            embedding_density=embedding_density,
            output_dimension=output_dimension,
            neighbor_influence=neighbor_influence,
            generate_features=generate_features,
            binarize_features=binarize_features,
            heterogeneous=heterogeneous,
            feature_properties=feature_properties,
            random_seed=random_seed,
        )

        return estimate_algorithm(
            query_runner=self._query_runner,
            endpoint="gds.hashgnn.stream.estimate",
            G=G,
            algo_config=config,
        )
