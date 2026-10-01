from typing import Any

from graphdatascience.error.cypher_warning_handler import filter_id_func_deprecation_warning
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType


def _escape_identifier(identifier: str) -> str:
    # Cypher escapes backticks inside quoted identifiers by doubling them.
    return identifier.replace("`", "``")


@filter_id_func_deprecation_warning()
def find_node_id(
    query_runner: QueryRunner,
    labels: list[str] | None = None,
    properties: dict[str, Any] | None = None,
) -> int:
    labels = labels or []
    properties = properties or {}
    label_pattern = "".join(f":`{_escape_identifier(label)}`" for label in labels)

    params: dict[str, Any] = {}
    property_entries: list[str] = []
    for i, (key, value) in enumerate(properties.items()):
        param_name = f"value_{i}"
        property_entries.append(f"`{_escape_identifier(key)}`: ${param_name}")
        params[param_name] = value

    property_pattern = f" {{{', '.join(property_entries)}}}" if property_entries else ""
    query = f"MATCH (n{label_pattern}{property_pattern}) RETURN id(n) AS id"

    node_match = query_runner.run_retryable_cypher(
        query, QueryType.USER_TRANSPILED, params, custom_error=False, mode=QueryMode.READ
    )

    if len(node_match) != 1:
        raise ValueError(f"Filter did not match with exactly one node: {node_match.to_string()}")

    return node_match["id"][0].item()  # type: ignore
