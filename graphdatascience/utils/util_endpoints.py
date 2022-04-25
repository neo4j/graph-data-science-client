from typing import Any, Dict, List

from pandas.core.frame import DataFrame

from ..caller_base import CallerBase
from ..error.client_only_endpoint import client_only_endpoint
from .util_proc_runner import UtilProcRunner


class UtilEndpoints(CallerBase):
    @property
    def util(self) -> UtilProcRunner:
        return UtilProcRunner(self._query_runner, f"{self._namespace}.util", self._server_version)

    @client_only_endpoint("gds")
    def find_node_id(self, labels: List[str] = [], properties: Dict[str, Any] = {}) -> int:
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
            raise ValueError(f"Filter did not match with exactly one node: {node_match}")

        return node_match["id"][0].item()  # type: ignore

    def version(self) -> str:
        namespace = self._namespace + ".version"
        result = self._query_runner.run_query(f"RETURN {namespace}() as version").squeeze()

        return result  # type: ignore

    def list(self) -> DataFrame:
        namespace = self._namespace + ".list"
        return self._query_runner.run_query(f"CALL {namespace}()")

    def oneHotEncoding(self, available_values: List[Any], selected_values: List[Any]) -> List[int]:
        namespace = self._namespace + ".oneHotEncoding"

        query = f"RETURN {namespace}($available_values, $selected_values) AS embedding"
        params = {
            "available_values": available_values,
            "selected_values": selected_values,
        }
        result = self._query_runner.run_query(query, params)

        return result["embedding"].squeeze()  # type: ignore
