from typing import Any, Dict, List

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..query_runner.query_runner import QueryRunner


class UtilProcRunner(UncallableNamespace, IllegalAttrChecker):
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    def asNode(self, node_id: int) -> Dict[str, Any]:
        self._namespace += ".asNode"
        result = self._query_runner.run_query(
            f"RETURN {self._namespace}({node_id}) AS node"
        )

        return result[0]["node"]  # type: ignore

    def asNodes(self, node_ids: List[int]) -> List[Dict[str, Any]]:
        self._namespace += ".asNodes"
        result = self._query_runner.run_query(
            f"RETURN {self._namespace}({node_ids}) AS nodes"
        )

        return result[0]["nodes"]  # type: ignore
