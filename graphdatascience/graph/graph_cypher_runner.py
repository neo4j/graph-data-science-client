from collections import namedtuple
from typing import Any, NamedTuple, Optional, Tuple

from pandas import Series

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..query_runner.query_runner import QueryRunner
from ..server_version.server_version import ServerVersion
from .graph_object import Graph


class NodeProperty(NamedTuple):
    name: str
    property_key: str
    default_value: Optional[Any] = None


class NodeProjection(NamedTuple):
    name: str
    source_label: str
    properties: Optional[list[NodeProperty]] = None


class RelationshipProperty(NamedTuple):
    name: str
    property_key: str
    default_value: Optional[Any] = None


class RelationshipProjection(NamedTuple):
    name: str
    source_type: str
    properties: Optional[list[RelationshipProperty]] = None


class MatchPart(NamedTuple):
    match: str = ""
    source_where: str = ""
    optional_match: str = ""
    optional_where: str = ""

    def __str__(self) -> str:
        return "\n".join(
            part
            for part in [
                self.match,
                self.source_where,
                self.optional_match,
                self.optional_where,
            ]
            if part
        )


class MatchPattern(NamedTuple):
    label_filter: str = ""
    left_arrow: str = ""
    type_filter: str = ""
    right_arrow: str = ""

    def __str__(self) -> str:
        return f"{self.left_arrow}{self.type_filter}{self.right_arrow}(target{self.label_filter})"


class GraphCypherRunner(IllegalAttrChecker):
    def __init__(self, query_runner: QueryRunner, namespace: str, server_version: ServerVersion) -> None:
        if server_version < ServerVersion(2, 4, 0):
            raise ValueError("The new Cypher projection is only supported since GDS 2.4.0.")
        super().__init__(query_runner, namespace, server_version)

    def project(
        self,
        graph_name: str,
        *,
        nodes: Any = None,
        relationships: Any = None,
        where: Optional[str] = None,
        allow_disconnected_nodes: bool = False,
        inverse: bool = False,
        combine_labels_with: str = "OR",
        **config: Any,
    ) -> Tuple[Graph, "Series[Any]"]:
        """
        Project a graph using Cypher projection.

        Parameters
        ----------
        graph_name : str
            The name of the graph to project.
        nodes : Any
            The nodes to project. If not specified, all nodes are projected.
        relationships : Any
            The relationships to project. If not specified, all relationships
            are projected.
        where : Optional[str]
            A Cypher WHERE clause to filter the nodes and relationships to
            project.
        allow_disconnected_nodes : bool
            Whether to allow disconnected nodes in the projected graph.
        inverse : bool
            Whether to project inverse relationships. The projected graph will
            be configured as NATURAL.
        combine_labels_with : str
            Whether to combine node labels with AND or OR. The default is AND.
            Allowed values are 'AND' and 'OR'.
        **config : Any
            Additional configuration for the projection.
        """

        query_params = {"graph_name": graph_name}

        data_config = {}

        nodes = self._node_projections_spec(nodes)
        rels = self._rel_projections_spec(relationships)

        match_part = MatchPart()
        match_pattern = MatchPattern(
            left_arrow="<-" if inverse else "-",
            right_arrow="-" if inverse else "->",
        )

        if nodes:
            if len(nodes) == 1 or combine_labels_with == "AND":
                match_pattern = match_pattern._replace(label_filter=f":{':'.join(spec.source_label for spec in nodes)}")

                projected_labels = [spec.name for spec in nodes]
                data_config["sourceNodeLabels"] = projected_labels
                data_config["targetNodeLabels"] = projected_labels

            elif combine_labels_with == "OR":
                source_labels_filter = " OR ".join(f"source:{spec.source_label}" for spec in nodes)
                target_labels_filter = " OR ".join(f"target:{spec.source_label}" for spec in nodes)
                if allow_disconnected_nodes:
                    match_part = match_part._replace(
                        source_where=f"WHERE {source_labels_filter}", optional_where=f"WHERE {target_labels_filter}"
                    )
                else:
                    match_part = match_part._replace(
                        source_where=f"WHERE ({source_labels_filter}) AND ({target_labels_filter})"
                    )

                data_config["sourceNodeLabels"] = "labels(source)"
                data_config["targetNodeLabels"] = "labels(target)"
            else:
                raise ValueError(f"Invalid value for combine_labels_with: {combine_labels_with}")

        if rels:
            if len(rels) == 1:
                rel_var = ""
                data_config["relationshipType"] = rels[0].source_type
            else:
                rel_var = "rel"
                data_config["relationshipTypes"] = "type(rel)"
            match_pattern = match_pattern._replace(
                type_filter=f"[{rel_var}:{'|'.join(spec.source_type for spec in rels)}]"
            )

        source = f"(source{match_pattern.label_filter})"
        if allow_disconnected_nodes:
            match_part = match_part._replace(
                match=f"MATCH {source}", optional_match=f"OPTIONAL MATCH (source){match_pattern}"
            )
        else:
            match_part = match_part._replace(match=f"MATCH {source}{match_pattern}")

        match_part = str(match_part)

        args = ["$graph_name", "source", "target"]

        if data_config:
            query_params["data_config"] = data_config
            args += ["$data_config"]

        if config:
            query_params["config"] = config
            args += ["$config"]

        return_part = f"RETURN {self._namespace}({', '.join(args)})"

        query = "\n".join(part for part in [match_part, return_part] if part)

        print(query)

        result = self._query_runner.run_query_with_logging(
            query,
            query_params,
        ).squeeze()

        return Graph(graph_name, self._query_runner, self._server_version), result

    def _node_projections_spec(self, spec: Any) -> list[NodeProjection]:
        if spec is None or spec is False:
            return []

        if isinstance(spec, str):
            spec = [spec]

        if isinstance(spec, list):
            return [self._node_projection_spec(node) for node in spec]

        if isinstance(spec, dict):
            return [self._node_projection_spec(node, name) for name, node in spec.items()]

        raise TypeError(f"Invalid node projection specification: {spec}")

    def _node_projection_spec(self, spec: Any, name: Optional[str] = None) -> NodeProjection:
        if isinstance(spec, str):
            return NodeProjection(name=name or spec, source_label=spec)

        raise TypeError(f"Invalid node projection specification: {spec}")

    def _node_properties_spec(self, properties: dict[str, Any]) -> list[NodeProperty]:
        raise TypeError(f"Invalid node projection specification: {properties}")

    def _rel_projections_spec(self, spec: Any) -> list[RelationshipProjection]:
        if spec is None or spec is False:
            return []

        if isinstance(spec, str):
            spec = [spec]

        if isinstance(spec, list):
            return [self._rel_projection_spec(node) for node in spec]

        if isinstance(spec, dict):
            return [self._rel_projection_spec(node, name) for name, node in spec.items()]

        raise TypeError(f"Invalid relationship projection specification: {spec}")

    def _rel_projection_spec(self, spec: Any, name: Optional[str] = None) -> RelationshipProjection:
        if isinstance(spec, str):
            return RelationshipProjection(name=name or spec, source_type=spec)

        raise TypeError(f"Invalid relationship projection specification: {spec}")

    def _rel_properties_spec(self, properties: dict[str, Any]) -> list[RelationshipProperty]:
        raise TypeError(f"Invalid relationship projection specification: {properties}")

    #
    # def estimate(self, *, nodes: Any, relationships: Any, **config: Any) -> "Series[Any]":
    #     pass
