from __future__ import annotations
from itertools import chain

import colorsys
import random
from types import TracebackType
from typing import Any, List, Optional, Type, Union
from uuid import uuid4

from pandas import Series

from ..call_parameters import CallParameters
from ..query_runner.query_runner import QueryRunner


class Graph:
    """
    A graph object that represents a graph in the graph catalog.
    It can be passed into algorithm endpoints to compute over the corresponding graph.
    It contains summary information about the graph.
    """

    def __init__(self, name: str, query_runner: QueryRunner):
        self._name = name
        self._query_runner = query_runner
        self._db = query_runner.database()

    def __enter__(self: Graph) -> Graph:
        return self

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.drop()

    def name(self) -> str:
        """
        Returns:
            the name of the graph
        """
        return self._name

    def _graph_info(self, yields: List[str] = []) -> "Series[Any]":
        yield_db = "database" in yields
        yields_with_db = yields if yield_db else yields + ["database"]

        info = self._query_runner.call_procedure(
            endpoint="gds.graph.list",
            params=CallParameters(graph_name=self._name),
            yields=yields_with_db,
            custom_error=False,
        )

        if len(info) == 0:
            raise ValueError(f"There is no projected graph named '{self.name()}'")
        if len(info) > 1:
            # for multiple dbs we can have the same graph name. But db + graph name is unique
            info = info[info["database"] == self._db]

        if not yield_db:
            info = info.drop(columns=["database"])

        return info.squeeze()  # type: ignore

    def database(self) -> str:
        """
        Returns:
            the name of the database the graph is stored in
        """
        return self._graph_info(["database"])  # type: ignore

    def configuration(self) -> "Series[Any]":
        """
        Returns:
            the configuration of the graph
        """
        return Series(self._graph_info(["configuration"]))

    def node_count(self) -> int:
        """
        Returns:
            the number of nodes in the graph
        """
        return self._graph_info(["nodeCount"])  # type: ignore

    def relationship_count(self) -> int:
        """
        Returns:
            the number of relationships in the graph
        """
        return self._graph_info(["relationshipCount"])  # type: ignore

    def node_labels(self) -> List[str]:
        """
        Returns:
            the node labels in the graph
        """
        return list(self._graph_info(["schema"])["nodes"].keys())

    def relationship_types(self) -> List[str]:
        """
        Returns:
            the relationship types in the graph
        """
        return list(self._graph_info(["schema"])["relationships"].keys())

    def node_properties(self, label: Optional[str] = None) -> Union["Series[str]", List[str]]:
        """
        Args:
            label: the node label to get the properties for

        Returns:
            the node properties for the given label

        """
        labels_to_props = self._graph_info(["schema"])["nodes"]

        if not label:
            return Series({key: list(val.keys()) for key, val in labels_to_props.items()})

        if label not in labels_to_props.keys():
            raise ValueError(f"There is no node label '{label}' projected onto '{self.name()}'")

        return list(labels_to_props[label].keys())

    def relationship_properties(self, type: Optional[str] = None) -> Union["Series[str]", List[str]]:
        """
        Args:
            type: the relationship type to get the properties for

        Returns:
            the relationship properties for the given type
        """
        types_to_props = self._graph_info(["schema"])["relationships"]

        if not type:
            return Series({key: list(val.keys()) for key, val in types_to_props.items()})

        if type not in types_to_props.keys():
            raise ValueError(f"There is no relationship type '{type}' projected onto '{self.name()}'")

        return list(types_to_props[type].keys())

    def degree_distribution(self) -> "Series[float]":
        """
        Returns:
            the degree distribution of the graph
        """
        return Series(self._graph_info(["degreeDistribution"]))

    def density(self) -> float:
        """
        Returns:
            the density of the graph
        """
        return self._graph_info(["density"])  # type: ignore

    def memory_usage(self) -> str:
        """
        Returns:
            the memory usage of the graph
        """
        return self._graph_info(["memoryUsage"])  # type: ignore

    def size_in_bytes(self) -> int:
        """
        Returns:
            the size of the graph in bytes
        """
        return self._graph_info(["sizeInBytes"])  # type: ignore

    def exists(self) -> bool:
        """
        Returns:
            whether the graph exists
        """
        result = self._query_runner.call_procedure(
            endpoint="gds.graph.exists",
            params=CallParameters(graph_name=self._name),
            custom_error=False,
        )
        return result.squeeze()["exists"]  # type: ignore

    def drop(self, failIfMissing: bool = False) -> "Series[str]":
        """
        Args:
            failIfMissing: whether to fail if the graph does not exist

        Returns:
            the result of the drop operation
        """
        result = self._query_runner.call_procedure(
            endpoint="gds.graph.drop",
            params=CallParameters(graph_name=self._name, failIfMissing=failIfMissing),
            custom_error=False,
        )

        return result.squeeze()  # type: ignore

    def creation_time(self) -> Any:  # neo4j.time.DateTime not exported
        """
        Returns:
            the creation time of the graph
        """
        return self._graph_info(["creationTime"])

    def modification_time(self) -> Any:  # neo4j.time.DateTime not exported
        """
        Returns:
            the modification time of the graph
        """
        return self._graph_info(["modificationTime"])

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}(name={self.name()}, "
            f"node_count={self.node_count()}, relationship_count={self.relationship_count()})"
        )

    def __repr__(self) -> str:
        yield_fields = [
            "graphName",
            "nodeCount",
            "relationshipCount",
            "database",
            "configuration",
            "schema",
            "memoryUsage",
        ]
        return f"{self.__class__.__name__}({self._graph_info(yields=yield_fields).to_dict()})"

    def visualize(
        self,
        node_count: int = 100,
        directed: bool = True,
        center_nodes: Optional[List[int]] = None,
        color_property: Optional[str] = None,
        size_property: Optional[str] = None,
        include_node_properties: Optional[List[str]] = None,
        rel_weight_property: Optional[str] = None,
        notebook: bool = True,
        px_height: int = 750,
        theme: str = "dark",
    ) -> Any:
        """
        Visualize the `Graph` in an interactive graphical interface.
        The graph will be sampled down to specified `node_count` to limit computationally expensive rendering.

        Args:
            node_count: number of nodes in the graph to be visualized
            directed: whether or not to display relationships as directed
            center_nodes: nodes around subgraph will be sampled, if sampling is necessary
            color_property: node property that determines node categories for coloring. Default is to use node labels
            size_property: node property that determines the size of nodes. Default is to compute a page rank for this
            include_node_properties: node properties to include for mouse-over inspection
            rel_weight_property: relationship property that determines width of relationships
            notebook: whether or not the code is run in a notebook
            px_height: the height of the graphic containing output the visualization
            theme: coloring theme for the visualization. "light" or "dark"

        Returns:
            an interactive graphical visualization of the specified graph
        """

        actual_node_properties = list(chain.from_iterable(self.node_properties().to_dict().values()))
        if (color_property is not None) and (color_property not in actual_node_properties):
            raise ValueError(f"There is no node property '{color_property}' in graph '{self._name}'")

        if size_property is not None and size_property not in actual_node_properties:
            raise ValueError(f"There is no node property '{size_property}' in graph '{self._name}'")

        if include_node_properties is not None:
            for prop in include_node_properties:
                if prop not in actual_node_properties:
                    raise ValueError(f"There is no node property '{prop}' in graph '{self._name}'")

        actual_rel_properties = list(chain.from_iterable(self.relationship_properties().to_dict().values()))
        if rel_weight_property is not None and rel_weight_property not in actual_rel_properties:
            raise ValueError(f"There is no relationship property '{rel_weight_property}' in graph '{self._name}'")

        if theme not in {"light", "dark"}:
            raise ValueError(f"Color `theme` '{theme}' is not allowed. Must be either 'light' or 'dark'")

        visual_graph = self._name
        if self.node_count() > node_count:
            visual_graph = str(uuid4())
            config = dict(samplingRatio=float(node_count) / self.node_count())

            if center_nodes is not None:
                config["startNodes"] = center_nodes

            self._query_runner.call_procedure(
                endpoint="gds.graph.sample.rwr",
                params=CallParameters(graph_name=visual_graph, fromGraphName=self._name, config=config),
                custom_error=False,
            )

        # Make sure we always have at least a size property so that we can run `gds.graph.nodeProperties.stream`
        if size_property is None:
            size_property = str(uuid4())
            self._query_runner.call_procedure(
                endpoint="gds.pageRank.mutate",
                params=CallParameters(graph_name=visual_graph, config=dict(mutateProperty=size_property)),
                custom_error=False,
            )
            clean_up_size_prop = True
        else:
            clean_up_size_prop = False

        node_properties = [size_property]
        if include_node_properties is not None:
            node_properties.extend(include_node_properties)

        if color_property is not None:
            node_properties.append(color_property)

        # Remove possible duplicates
        node_properties = list(set(node_properties))

        result = self._query_runner.call_procedure(
            endpoint="gds.graph.nodeProperties.stream",
            params=CallParameters(
                graph_name=visual_graph,
                properties=node_properties,
                nodeLabels=self.node_labels(),
                config=dict(listNodeLabels=True),
            ),
            custom_error=False,
        )

        # new format was requested, but the query was run via Cypher
        if "propertyValue" in result.keys():
            wide_result = result.pivot(index=["nodeId"], columns=["nodeProperty"], values="propertyValue")
            # nodeLabels cannot be an index column of the pivot as its not hashable
            # so we need to manually join it back in
            labels_df = result[["nodeId", "nodeLabels"]].set_index("nodeId")
            wide_result = wide_result.join(labels_df, on="nodeId")
            result = wide_result.reset_index()
            result.columns.name = None
        node_properties_df = result

        if rel_weight_property is None:
            relationships_df = self._query_runner.call_procedure(
                endpoint="gds.graph.relationships.stream",
                params=CallParameters(graph_name=visual_graph),
                custom_error=False,
            )
        else:
            relationships_df = self._query_runner.call_procedure(
                endpoint="gds.graph.relationshipProperty.stream",
                params=CallParameters(graph_name=visual_graph, properties=rel_weight_property),
                custom_error=False,
            )

        # Clean up
        if visual_graph != self._name:
            self._query_runner.call_procedure(
                endpoint="gds.graph.drop",
                params=CallParameters(graph_name=visual_graph),
                custom_error=False,
            )
        elif clean_up_size_prop:
            self._query_runner.call_procedure(
                endpoint="gds.graph.nodeProperties.drop",
                params=CallParameters(graph_name=visual_graph, nodeProperties=size_property),
                custom_error=False,
            )

        from pyvis.network import Network

        net = Network(
            notebook=True if notebook else False,
            cdn_resources="remote" if notebook else "local",
            directed=directed,
            bgcolor="#222222" if theme == "dark" else "#F2F2F2",
            font_color="white" if theme == "dark" else "black",
            height=f"{px_height}px",
            width="100%",
        )

        if color_property is None:
            color_map = {label: self._random_themed_color(theme) for label in self.node_labels()}
        else:
            color_map = {
                prop_val: self._random_themed_color(theme) for prop_val in node_properties_df[color_property].unique()
            }

        # Add all the nodes
        for _, node in node_properties_df.iterrows():
            title = f"Node ID: {node['nodeId']}\nLabels: {node['nodeLabels']}"
            if include_node_properties is not None:
                title += f"\nNode properties:"
                for prop in include_node_properties:
                    title += f"\n{prop} = {node[prop]}"

            if color_property is None:
                color = color_map[node["nodeLabels"][0]]
            else:
                color = color_map[node[color_property]]

            net.add_node(
                int(node["nodeId"]),
                value=node[size_property],
                color=color,
                title=title,
            )

        # Add all the relationships
        for _, rel in relationships_df.iterrows():
            if rel_weight_property is None:
                net.add_edge(rel["sourceNodeId"], rel["targetNodeId"], title=f"Type: {rel['relationshipType']}")
            else:
                title = f"Type: {rel['relationshipType']}\n{rel_weight_property} = {rel['rel_weight_property']}"
                net.add_edge(rel["sourceNodeId"], rel["targetNodeId"], title=title, value=rel[rel_weight_property])

        return net.show(f"{self._name}.html")

    @staticmethod
    def _random_themed_color(theme) -> str:
        l = 0.7 if theme == "dark" else 0.4
        return "#%02X%02X%02X" % tuple(map(lambda x: int(x * 255), colorsys.hls_to_rgb(random.random(), l, 1.0)))
