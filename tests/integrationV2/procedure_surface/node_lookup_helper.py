from graphdatascience.error.cypher_warning_handler import filter_id_func_deprecation_warning
from graphdatascience.query_runner.query_runner import QueryRunner


@filter_id_func_deprecation_warning()
def find_node_by_id(query_runner: QueryRunner, id: int) -> int:
    return int(
        query_runner.run_cypher(
            "MATCH (n:Node {id: $id}) RETURN id(n) AS node",
            params={"id": id},
        ).iloc[0]["node"]
    )


@filter_id_func_deprecation_warning()
def find_node_by_name(query_runner: QueryRunner, name: str) -> int:
    return int(
        query_runner.run_cypher(  # type: ignore
            "MATCH (n:Node {name: $name}) RETURN id(n) AS node",
            params={"name": name},
        ).iloc[0]["node"]
    )
