from typing import Any, Dict, List

from ..query_runner.query_runner import QueryRunner


class UtilEndpoints:
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    def find_node_id(
        self, labels: List[str] = [], properties: Dict[str, Any] = {}
    ) -> int:

        label_match = None
        if labels:
            label_match = " AND ".join([f"n:{label}" for label in labels])

        prop_match = None
        if properties:
            parsed_props = []
            for key, val in properties.items():
                if isinstance(val, str):
                    parsed_props.append(f"n.{key} = '{val}'")
                else:
                    parsed_props.append(f"n.{key} = {val}")
            prop_match = " AND ".join(parsed_props)

        if label_match and prop_match:
            query = f"MATCH (n) WHERE {label_match} AND {prop_match} RETURN id(n) AS id"
        elif label_match:
            query = f"MATCH (n) WHERE {label_match} RETURN id(n) AS id"
        elif prop_match:
            query = f"MATCH (n) WHERE {prop_match} RETURN id(n) AS id"
        else:
            query = "MATCH (n) RETURN id(n) AS id"

        node_match = self._query_runner.run_query(query)
        if len(node_match) != 1:
            raise ValueError(
                f"Filter did not match with exactly one node: {node_match}"
            )

        return node_match[0]["id"]  # type: ignore
