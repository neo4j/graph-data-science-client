import os
from typing import List, Optional, Union

from pandas import DataFrame

from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion
from .graph_alpha_project_runner import GraphAlphaProjectRunner
from .graph_entity_ops_runner import GraphLabelRunner, GraphPropertyRunner
from .graph_object import Graph
from .graph_sample_runner import GraphSampleRunner


class GraphAlphaProcRunner(UncallableNamespace, IllegalAttrChecker):
    @property
    def sample(self) -> GraphSampleRunner:
        self._namespace += ".sample"
        return GraphSampleRunner(self._query_runner, self._namespace, self._server_version)

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
    @compatible_with("construct", min_inclusive=ServerVersion(2, 1, 0))
    def construct(
        self,
        graph_name: str,
        nodes: Union[DataFrame, List[DataFrame]],
        relationships: Union[DataFrame, List[DataFrame]],
        concurrency: int = 4,
        undirected_relationship_types: Optional[List[str]] = None,
    ) -> Graph:
        nodes = nodes if isinstance(nodes, List) else [nodes]
        relationships = relationships if isinstance(relationships, List) else [relationships]

        errors = []

        exists = self._query_runner.run_query(
            f"CALL gds.graph.exists('{graph_name}') YIELD exists", custom_error=False
        ).squeeze()

        # compare against True as (1) unit tests return None here and (2) numpys True does not work with `is True`.
        if exists == True:  # noqa: E712
            errors.append(
                f"Graph '{graph_name}' already exists. Please drop the existing graph or use a different name."
            )

        for idx, node_df in enumerate(nodes):
            if "nodeId" not in node_df.columns.values:
                errors.append(f"Node dataframe at index {idx} needs to contain a 'nodeId' column.")

        for idx, rel_df in enumerate(relationships):
            for expected_col in ["sourceNodeId", "targetNodeId"]:
                if expected_col not in rel_df.columns.values:
                    errors.append(f"Relationship dataframe at index {idx} needs to contain a '{expected_col}' column.")

        if self._server_version < ServerVersion(2, 3, 0) and undirected_relationship_types:
            errors.append("The parameter 'undirected_relationship_types' is only supported since GDS 2.3.0.")

        if len(errors) > 0:
            raise ValueError(os.linesep.join(errors))

        constructor = self._query_runner.create_graph_constructor(
            graph_name, concurrency, undirected_relationship_types
        )
        constructor.run(nodes, relationships)

        return Graph(graph_name, self._query_runner, self._server_version)
