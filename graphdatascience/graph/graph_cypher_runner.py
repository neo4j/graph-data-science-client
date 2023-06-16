from collections import defaultdict
from typing import Any, Dict, NamedTuple, Optional, Tuple

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


class MatchParts(NamedTuple):
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


class LabelPropertyMapping(NamedTuple):
    label: str
    property_key: str
    default_value: Optional[Any] = None


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

        Returns
        -------
        A tuple of the projected graph and statistics about the projection
        """

        query_params: Dict[str, Any] = {"graph_name": graph_name}

        data_config: Dict[str, Any] = {}
        data_config_is_static = True

        nodes = self._node_projections_spec(nodes)
        rels = self._rel_projections_spec(relationships)

        match_parts = MatchParts()
        match_pattern = MatchPattern(
            left_arrow="<-" if inverse else "-",
            right_arrow="-" if inverse else "->",
        )

        label_mappings = defaultdict(list)

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
                    match_parts = match_parts._replace(
                        source_where=f"WHERE {source_labels_filter}", optional_where=f"WHERE {target_labels_filter}"
                    )
                else:
                    match_parts = match_parts._replace(
                        source_where=f"WHERE ({source_labels_filter}) AND ({target_labels_filter})"
                    )

                data_config["sourceNodeLabels"] = "labels(source)"
                data_config["targetNodeLabels"] = "labels(target)"
                data_config_is_static = False
            else:
                raise ValueError(f"Invalid value for combine_labels_with: {combine_labels_with}")

            for spec in nodes:
                if spec.properties:
                    for prop in spec.properties:
                        label_mappings[spec.source_label].append(
                            LabelPropertyMapping(spec.source_label, prop.property_key, prop.default_value)
                        )

        rel_var = ""
        if rels:
            if len(rels) == 1:
                data_config["relationshipType"] = rels[0].source_type
            else:
                rel_var = "rel"
                data_config["relationshipTypes"] = "type(rel)"
                data_config_is_static = False

            match_pattern = match_pattern._replace(
                type_filter=f"[{rel_var}:{'|'.join(spec.source_type for spec in rels)}]"
            )

        source = f"(source{match_pattern.label_filter})"
        if allow_disconnected_nodes:
            match_parts = match_parts._replace(
                match=f"MATCH {source}", optional_match=f"OPTIONAL MATCH (source){match_pattern}"
            )
        else:
            match_parts = match_parts._replace(match=f"MATCH {source}{match_pattern}")

        match_part = str(match_parts)

        print("nodes", nodes)
        print("labels", label_mappings)

        case_part = []
        if label_mappings:
            with_rel = f", {rel_var}" if rel_var else ""
            case_part = [f"WITH source, target{with_rel}"]
            for kind in ["source", "target"]:
                case_part.append("CASE")

                for label, mappings in label_mappings.items():
                    mapping_projection = ", ".join(f".{key.property_key}" for key in mappings)
                    when_part = f"WHEN '{label}' in labels({kind}) THEN [{kind} {{{mapping_projection}}}]"
                    case_part.append(when_part)

                case_part.append(f"END AS {kind}NodeProperties")

            data_config["sourceNodeProperties"] = "sourceNodeProperties"
            data_config["targetNodeProperties"] = "targetNodeProperties"
            data_config_is_static = False

        args = ["$graph_name", "source", "target"]

        if data_config:
            if data_config_is_static:
                query_params["data_config"] = data_config
                args += ["$data_config"]
            else:
                args += [self._render_map(data_config)]

        if config:
            query_params["config"] = config
            args += ["$config"]

        return_part = f"RETURN {self._namespace}({', '.join(args)})"

        query = "\n".join(part for part in [match_part, *case_part, return_part] if part)

        result = self._query_runner.run_query_with_logging(query, query_params)
        result = result.squeeze()

        return Graph(graph_name, self._query_runner, self._server_version), result  # type: ignore

    def _node_projections_spec(self, spec: Any) -> list[NodeProjection]:
        if spec is None or spec is False:
            return []

        if isinstance(spec, str):
            spec = [spec]

        if isinstance(spec, list):
            return [self._node_projection_spec(node) for node in spec]

        if isinstance(spec, dict):
            return [self._node_projection_spec(node, name) for name, node in spec.items()]

        raise TypeError(f"Invalid node projections specification: {spec}")

    def _node_projection_spec(self, spec: Any, name: Optional[str] = None) -> NodeProjection:
        if isinstance(spec, str):
            return NodeProjection(name=name or spec, source_label=spec)

        if name is None:
            raise ValueError(f"Node projections with properties must use the dict syntax: {spec}")

        if isinstance(spec, dict):
            properties = [self._node_properties_spec(prop, name) for name, prop in spec.items()]
            return NodeProjection(name=name, source_label=name, properties=properties)

        if isinstance(spec, list):
            properties = [self._node_properties_spec(prop) for prop in spec]
            return NodeProjection(name=name, source_label=name, properties=properties)

        raise TypeError(f"Invalid node projection specification: {spec}")

    def _node_properties_spec(self, spec: Any, name: Optional[str] = None) -> NodeProperty:
        if isinstance(spec, str):
            return NodeProperty(name=name or spec, property_key=spec)

        if isinstance(spec, dict):
            name = spec.pop("name", name)
            if name is None:
                raise ValueError(
                    f"Node properties must specify either a name in the outer dict or by using the `name` key: {spec}"
                )
            property_key = spec.pop("property_key", name)

            return NodeProperty(name=name, property_key=property_key, **spec)

        if spec is True:
            if name is None:
                raise ValueError(f"Node properties spec must be used with the dict syntax: {spec}")

            return NodeProperty(name=name, property_key=name)

        raise TypeError(f"Invalid node property specification: {spec}")

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

    def _rel_properties_spec(self, properties: Dict[str, Any]) -> list[RelationshipProperty]:
        raise TypeError(f"Invalid relationship projection specification: {properties}")

    def _render_map(self, mapping: Dict[str, Any]) -> str:
        return "{" + ", ".join(f"{key}: {value}" for key, value in mapping.items()) + "}"

    #
    # def estimate(self, *, nodes: Any, relationships: Any, **config: Any) -> "Series[Any]":
    #     pass
