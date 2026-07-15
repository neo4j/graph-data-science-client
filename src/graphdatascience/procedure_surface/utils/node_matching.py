from typing import Any

from graphdatascience.error.cypher_warning_handler import filter_id_func_deprecation_warning
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType


@filter_id_func_deprecation_warning()
def find_node_id(
    query_runner: QueryRunner,
    labels: list[str] | None = None,
    properties: dict[str, Any] | None = None,
) -> int:
    labels = labels or []
    properties = properties or {}

    conditions = [f"n:`{label}`" for label in labels]

    # Property values are passed as query parameters to avoid injection and quoting issues.
    params: dict[str, Any] = {}
    for i, (key, value) in enumerate(properties.items()):
        param_name = f"value_{i}"
        conditions.append(f"n.`{key}` = ${param_name}")
        params[param_name] = value

    where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    query = f"MATCH (n){where_clause} RETURN id(n) AS id"

    node_match = query_runner.run_retryable_cypher(
        query, QueryType.USER_TRANSPILED, params, custom_error=False, mode=QueryMode.READ
    )

    if len(node_match) != 1:
        raise ValueError(f"Filter did not match with exactly one node: {node_match}")

    return node_match["id"][0].item()  # type: ignore
