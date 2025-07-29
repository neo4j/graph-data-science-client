from collections import OrderedDict
from typing import Any, Optional, Union

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.graph_object import Graph
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


def estimate_algorithm(
    endpoint: str,
    query_runner: QueryRunner,
    G: Optional[Graph] = None,
    projection_config: Optional[dict[str, Any]] = None,
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
    G : Optional[Graph], optional
        The graph to be used in the estimation
    projection_config : Optional[dict[str, Any]], optional
        Configuration dictionary for the projection

    Returns
    -------
    EstimationResult
        An object containing the result of the estimation

    Raises
    ------
    ValueError
        If neither G nor projection_config is provided
    """
    config: Union[dict[str, Any]] = OrderedDict()

    if G is not None:
        config["graphNameOrConfiguration"] = G.name()
    elif projection_config is not None:
        config["graphNameOrConfiguration"] = projection_config
    else:
        raise ValueError("Either graph_name or projection_config must be provided.")

    config["algoConfig"] = {}

    params = CallParameters(**config)

    result = query_runner.call_procedure(endpoint=endpoint, params=params).squeeze()

    return EstimationResult(**result.to_dict())
