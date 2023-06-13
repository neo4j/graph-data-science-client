from typing import List, Optional, Union

from pandas import DataFrame

from ..error.client_only_endpoint import client_only_endpoint
from ..error.deprecation_warning import deprecation_warning
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion
from .graph_alpha_project_runner import GraphAlphaProjectRunner
from .graph_entity_ops_runner import GraphLabelRunner, GraphPropertyRunner
from .graph_object import Graph
from .graph_proc_runner import GraphProcRunner
from .graph_sample_runner import GraphAlphaSampleRunner


class GraphAlphaProcRunner(UncallableNamespace, IllegalAttrChecker):
    @property
    def sample(self) -> GraphAlphaSampleRunner:
        self._namespace += ".sample"
        return GraphAlphaSampleRunner(self._query_runner, self._namespace, self._server_version)

    @property
    def graphProperty(self) -> GraphPropertyRunner:
        self._namespace += ".graphProperty"
        return GraphPropertyRunner(self._query_runner, self._namespace, self._server_version)

    @property
    def nodeLabel(self) -> GraphLabelRunner:
        self._namespace += ".nodeLabel"
        return GraphLabelRunner(self._query_runner, self._namespace, self._server_version)

    @property
    def project(self) -> GraphAlphaProjectRunner:
        self._namespace += ".project"
        return GraphAlphaProjectRunner(self._query_runner, self._namespace, self._server_version)

    @client_only_endpoint("gds.alpha.graph")
    @deprecation_warning("gds.graph", ServerVersion(2, 1, 0))
    @compatible_with("construct", min_inclusive=ServerVersion(2, 1, 0))
    def construct(
        self,
        graph_name: str,
        nodes: Union[DataFrame, List[DataFrame]],
        relationships: Union[DataFrame, List[DataFrame]],
        concurrency: int = 4,
        undirected_relationship_types: Optional[List[str]] = None,
    ) -> Graph:
        graph_proc_runner = GraphProcRunner(self._query_runner, f"{self._namespace}.graph", self._server_version)
        return graph_proc_runner.construct(graph_name, nodes, relationships, concurrency, undirected_relationship_types)
