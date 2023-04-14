from typing import Any, Dict, Optional

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace


class TopologicalLPAlphaRunner(UncallableNamespace, IllegalAttrChecker):
    def _run_standard_function(self, node1: int, node2: int, config: Dict[str, Any]) -> float:
        query = f"""
        MATCH (n1) WHERE id(n1) = {node1}
        MATCH (n2) WHERE id(n2) = {node2}
        RETURN {self._namespace}(n1, n2, $config) AS score
        """
        params = {"config": config}

        return self._query_runner.run_query(query, params)["score"].squeeze()  # type: ignore

    def adamicAdar(self, node1: int, node2: int, **config: Any) -> float:
        """
        Calculate the Adamic-Adar similarity between two nodes.

        Args:
            node1: Id of node1.
            node2: Id of node2.
            **config: Additional configuration parameters.

        Returns:
            Adamic-Adar similarity between node1 and node2.

        """
        self._namespace += ".adamicAdar"
        return self._run_standard_function(node1, node2, config)

    def commonNeighbors(self, node1: int, node2: int, **config: Any) -> float:
        """
        Calculate the number of common neighbors between two nodes.

        Args:
            node1: Id of node1.
            node2: Id of node2.
            **config: Additional configuration parameters.

        Returns:
            Number of common neighbors between node1 and node2.

        """
        self._namespace += ".commonNeighbors"
        return self._run_standard_function(node1, node2, config)

    def preferentialAttachment(self, node1: int, node2: int, **config: Any) -> float:
        """
        Calculate the preferential attachment between two nodes.

        Args:
            node1: Id of node1.
            node2: Id of node2.
            **config: Additional configuration parameters.

        Returns:
            Preferential attachment between node1 and node2.

        """
        self._namespace += ".preferentialAttachment"
        return self._run_standard_function(node1, node2, config)

    def resourceAllocation(self, node1: int, node2: int, **config: Any) -> float:
        """
        Calculate the resource allocation between two nodes.

        Args:
            node1: Id of node1.
            node2: Id of node2.
            **config: Additional configuration parameters.

        Returns:
            Resource allocation between node1 and node2.

        """
        self._namespace += ".resourceAllocation"
        return self._run_standard_function(node1, node2, config)

    def sameCommunity(self, node1: int, node2: int, communityProperty: Optional[str] = None) -> float:
        """
        Calculate the same community between two nodes.

        Args:
            node1: Id of node1.
            node2: Id of node2.
            communityProperty: Property to use for community detection.

        Returns:
            Same community between node1 and node2.

        """
        self._namespace += ".sameCommunity"
        community_property = f", '{communityProperty}'" if communityProperty else ""

        query = f"""
        MATCH (n1) WHERE id(n1) = {node1}
        MATCH (n2) WHERE id(n2) = {node2}
        RETURN {self._namespace}(n1, n2{community_property}) AS score
        """

        return self._query_runner.run_query(query)["score"].squeeze()  # type: ignore

    def totalNeighbors(self, node1: int, node2: int, **config: Any) -> float:
        """
        Calculate the total number of neighbors between two nodes.

        Args:
            node1: Id of node1.
            node2: Id of node2.
            **config: Additional configuration parameters.

        Returns:
            Total number of neighbors between node1 and node2.

        """
        self._namespace += ".totalNeighbors"
        return self._run_standard_function(node1, node2, config)
