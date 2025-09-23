from typing import Any, Dict, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.node_embedding.hashgnn_endpoints import (
    HashGNNEndpoints,
    HashGNNMutateResult,
    HashGNNWriteResult,
)

from ...call_parameters import CallParameters
from ...query_runner.query_runner import QueryRunner
from ..api.estimation_result import EstimationResult
from ..utils.config_converter import ConfigConverter
from .estimation_utils import estimate_algorithm


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
        output_dimension: Optional[int] = None,
        neighbor_influence: Optional[float] = None,
        generate_features: Optional[Dict[str, Any]] = None,
        binarize_features: Optional[Dict[str, Any]] = None,
        heterogeneous: Optional[bool] = None,
        feature_properties: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
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

        result = self._query_runner.call_procedure(endpoint="gds.hashgnn.mutate", params=params).squeeze()

        return HashGNNMutateResult(**result.to_dict())

    def stream(
        self,
        G: GraphV2,
        iterations: int,
        embedding_density: int,
        output_dimension: Optional[int] = None,
        neighbor_influence: Optional[float] = None,
        generate_features: Optional[Dict[str, Any]] = None,
        binarize_features: Optional[Dict[str, Any]] = None,
        heterogeneous: Optional[bool] = None,
        feature_properties: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
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

        return self._query_runner.call_procedure(endpoint="gds.hashgnn.stream", params=params)

    def write(
        self,
        G: GraphV2,
        iterations: int,
        embedding_density: int,
        write_property: str,
        output_dimension: Optional[int] = None,
        neighbor_influence: Optional[float] = None,
        generate_features: Optional[Dict[str, Any]] = None,
        binarize_features: Optional[Dict[str, Any]] = None,
        heterogeneous: Optional[bool] = None,
        feature_properties: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        write_concurrency: Optional[int] = None,
        random_seed: Optional[int] = None,
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

        result = self._query_runner.call_procedure(endpoint="gds.hashgnn.write", params=params).squeeze()

        return HashGNNWriteResult(**result.to_dict())

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        iterations: int,
        embedding_density: int,
        output_dimension: Optional[int] = None,
        neighbor_influence: Optional[float] = None,
        generate_features: Optional[Dict[str, Any]] = None,
        binarize_features: Optional[Dict[str, Any]] = None,
        heterogeneous: Optional[bool] = None,
        feature_properties: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
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
