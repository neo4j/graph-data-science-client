from graphdatascience import QueryRunner


def delete_all_graphs(query_runner: QueryRunner) -> None:
    query_runner.run_cypher(
        "CALL gds.graph.list() YIELD graphName CALL gds.graph.drop(graphName) YIELD graphName as g RETURN g"
    )
