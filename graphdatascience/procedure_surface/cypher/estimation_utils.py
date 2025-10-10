from collections import OrderedDict
from typing import Any

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.query_runner.query_runner import QueryRunner


def estimate_algorithm(
    endpoint: str,
    query_runner: QueryRunner,
    G: GraphV2 | dict[str, Any],
    algo_config: dict[str, Any] | None = None,
) -> EstimationResult:
    """
    Estimate the memory consumption of an algorithm run.

    This utility function provides a common implementation for estimation
    across all cypher endpoint implementations.

    Parameters
    ----------
    query_runner : QueryRunner
        The query runner to use for the estimation call
    endpoint : str
        The full endpoint name for the estimation procedure (e.g., "gds.kcore.stats.estimate")
    G : GraphV2 | None, optional
        The graph to be used in the estimation
    projection_config : dict[str, Any] | None, optional
        Configuration dictionary for the projection
    algo_config : dict[str, Any] | None, optional
        Additional algorithm-specific configuration parameters

    Returns
    -------
    EstimationResult
        An object containing the result of the estimation

    Raises
    ------
    ValueError
        If neither G nor projection_config is provided
    """
    config: dict[str, Any] = OrderedDict()

    if isinstance(G, GraphV2):
        config["graphNameOrConfiguration"] = G.name()
    elif isinstance(G, dict):
        config["graphNameOrConfiguration"] = G
    else:
        raise ValueError(f"G must be either a GraphV2 instance or a configuration dictionary. But was {type(G)}.")

    config["algoConfig"] = algo_config or {}

    params = CallParameters(**config)

    result = query_runner.call_procedure(endpoint=endpoint, params=params).squeeze()

    return EstimationResult(**result.to_dict())
