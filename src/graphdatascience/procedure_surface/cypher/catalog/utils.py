from graphdatascience.call_parameters import CallParameters
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.query_runner.query_runner import QueryRunner

# Fields of `GraphInfo`. Yielded explicitly to skip the deprecated `schema` column,
# which the server warns about whenever it is returned.
GRAPH_INFO_YIELDS = [
    "graphName",
    "database",
    "databaseLocation",
    "configuration",
    "memoryUsage",
    "sizeInBytes",
    "nodeCount",
    "relationshipCount",
    "creationTime",
    "modificationTime",
    "schemaWithOrientation",
    "density",
]

# Fields of `GraphInfoWithDegrees`.
GRAPH_INFO_WITH_DEGREES_YIELDS = GRAPH_INFO_YIELDS + ["degreeDistribution"]


def require_database(query_runner: QueryRunner) -> str:
    database = query_runner.database()
    if database is None:
        raise ValueError(
            "For this call you must have explicitly specified a valid Neo4j database to target, "
            "using `gds.set_database`."
        )

    return database


def drop_graph_if_exists(cypher_runner: QueryRunner, graph_name: str) -> None:
    """Idempotently drop a graph, ignoring it if it does not exist."""
    cypher_runner.call_procedure(
        endpoint="gds.graph.drop",
        params=CallParameters(graphName=graph_name, failIfMissing=False),
        yields=GRAPH_INFO_YIELDS,
        # dropping is idempotent as long as a missing graph is not an error
        retryable=True,
        mode=QueryMode.WRITE,
    )
