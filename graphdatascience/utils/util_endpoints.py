from typing import Any, Dict, List

from pandas import DataFrame

from ..caller_base import CallerBase
from ..error.client_only_endpoint import client_only_endpoint
from .util_proc_runner import UtilProcRunner
from graphdatascience.call_parameters import CallParameters
from graphdatascience.error.cypher_warning_handler import (
    filter_id_func_deprecation_warning,
)
from graphdatascience.server_version.server_version import ServerVersion


class DirectUtilEndpoints(CallerBase):
    @client_only_endpoint("gds")
    @filter_id_func_deprecation_warning()
    def find_node_id(self, labels: List[str] = [], properties: Dict[str, Any] = {}) -> int:
        """
        Find the node id of a node with the given labels and properties.

        Args:
            labels: The labels of the node to find.
            properties: The properties of the node to find.

        Returns:
            The node id of the node with the given labels and properties.
        """
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

        node_match = self._query_runner.run_cypher(query, custom_error=False)

        if len(node_match) != 1:
            raise ValueError(f"Filter did not match with exactly one node: {node_match}")

        return node_match["id"][0].item()  # type: ignore

    def version(self) -> str:
        """
        Get the version of the GDS library.

        Returns:
            The version of the GDS library.
        """
        return f"{self._server_version}"

    @client_only_endpoint("gds")
    def server_version(self) -> ServerVersion:
        """
        Get the version of the GDS library.

        Returns:
            The version of the GDS library.

        """
        return ServerVersion.from_string(self.version())

    def list(self) -> DataFrame:
        """
        List all available GDS procedures.

        Returns:
            A DataFrame containing all available GDS procedures.
        """
        namespace = self._namespace + ".list"
        return self._query_runner.call_procedure(endpoint=namespace, custom_error=False)

    @property
    def util(self) -> UtilProcRunner:
        return UtilProcRunner(self._query_runner, f"{self._namespace}.util", self._server_version)


class IndirectUtilAlphaEndpoints(CallerBase):
    def oneHotEncoding(self, available_values: List[Any], selected_values: List[Any]) -> List[int]:
        """
        One hot encode a list of values.

        Args:
            available_values: The available values to encode.
            selected_values: The values to encode.

        Returns:
            The one hot encoded values.
        """
        namespace = self._namespace + ".oneHotEncoding"

        params = CallParameters(
            available_values=available_values,
            selected_values=selected_values,
        )
        query = f"RETURN {namespace}($available_values, $selected_values) AS encoded"
        result = self._query_runner.run_cypher(query=query, params=params)

        return result.iat[0, 0]  # type: ignore
