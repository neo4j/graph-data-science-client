from functools import reduce

from pandas import DataFrame

from graphdatascience import QueryRunner
from graphdatascience.query_runner.query_mode import QueryMode


def transpose_property_columns(result: DataFrame, list_node_labels: bool) -> DataFrame:
    wide_result = result.pivot(index=["nodeId"], columns=["nodeProperty"], values="propertyValue")
    if list_node_labels:
        labels_df = result[["nodeId", "nodeLabels"]]
        # drop duplicates so that we only have a single row for each node id
        labels_df = labels_df.drop_duplicates(ignore_index=False, subset=["nodeId"])
        labels_df.set_index("nodeId", inplace=True)

        wide_result = wide_result.join(labels_df, on="nodeId")
    wide_result = wide_result.reset_index()
    wide_result.columns.name = None

    return wide_result

def join_db_node_properties(result: DataFrame, db_node_properties: list[str], query_runner: QueryRunner) -> DataFrame:
    query = _build_query(db_node_properties)
    db_properties_df = query_runner.run_retryable_cypher(
        query,
        params={"ids": (result["nodeId"].tolist())},
        mode=QueryMode.READ,
    )

    return result.merge(db_properties_df.set_index("nodeId"), on="nodeId", how='left')

def _build_query(db_node_properties: list[str]) -> str:
    query_prefix = "MATCH (n) WHERE id(n) IN $ids RETURN id(n) AS nodeId"

    def add_property(query: str, prop: str) -> str:
        return f"{query}, n.`{prop}` AS `{prop}`"

    return reduce(add_property, db_node_properties, query_prefix)