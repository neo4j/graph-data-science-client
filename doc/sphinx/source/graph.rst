Graph operations
----------------

.. py:function:: gds.alpha.graph.graphProperty.drop(self, G: Graph, graph_property: str, **config: Any) -> "Series[Any]"

    Removes a graph property from a projected graph.

.. py:function:: gds.alpha.graph.graphProperty.stream(self, G: Graph, graph_property: str, **config: Any) -> DataFrame

    Streams the given graph property.

.. py:function:: gds.alpha.graph.nodeLabel.mutate(self, G: Graph, node_label: str, **config: Any) -> "Series[Any]"

    Mutates the in-memory graph with the given node Label.

.. py:function:: gds.alpha.graph.nodeLabel.write(self, G: Graph, node_label: str, **config: Any) -> "Series[Any]"

    Writes the given node Label to an online Neo4j database.

.. py:function:: gds.beta.graph.export.csv(self, G: Graph, **config: Any) -> "Series[Any]"

    Exports a named graph to CSV files.

.. py:function:: gds.beta.graph.export.csv.estimate(self, G: Graph, **config: Any) -> "Series[Any]

    Estimate the required disk space for exporting a named graph to CSV files.


.. py:function:: gds.beta.graph.generate(self, graph_name: str, node_count: int, average_degree: int, **config: Any) -> Tuple[Graph, "Series[Any]"]

    Computes a random graph, which will be stored in the graph catalog.

.. py:function:: gds.beta.graph.project.subgraph(self,graph_name: str,from_G: Graph,node_filter: str,relationship_filter: str,**config: Any,) -> Tuple[Graph, "Series[Any]"]

    Creates a named graph in the catalog for use by algorithms.

.. py:function:: gds.beta.graph.relationships.stream(self, G: Graph, relationship_types: List[str] = ["*"], **config: Any) -> TopologyDataFrame

    Streams the given relationship source/target pairs

.. py:function:: gds.beta.graph.relationships.toUndirected(self, G: Graph, query: str, relationship_type: str, mutate_relationship_type: str, **config: Any) -> "Series[Any]"

    The ToUndirected procedure converts directed relationships to undirected relationships

.. py:function:: gds.beta.graph.relationships.toUndirected.estimate(self, G: Graph, relationship_type: str, mutate_relationship_type: str, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.graph.deleteRelationships(self, G: Graph, relationship_type: str) -> "Series[Any]"

    Delete the relationship type for a given graph stored in the graph-catalog.

.. py:function:: gds.graph.drop(self,G: Graph,failIfMissing: bool = False,dbName: str = "",username: Optional[str] = None,) -> Optional["Series[Any]"]

    Drops a named graph from the catalog and frees up the resources it occupies.

.. py:function:: gds.graph.exists(self, graph_name: str) -> "Series[Any]"

    Checks if a graph exists in the catalog.

.. py:function:: gds.graph.export(self, G: Graph, **config: Any) -> "Series[Any]"

    Exports a named graph into a new offline Neo4j database.

.. py:function:: gds.graph.list(self, G: Optional[Graph] = None) -> DataFrame

    Lists information about named graphs stored in the catalog.

.. py:function:: gds.graph.nodeProperties.drop(self, G: Graph, node_properties: List[str], **config: Any) -> "Series[Any]"

    Removes node properties from a projected graph.

.. py:function:: gds.graph.nodeProperties.stream(self,G: Graph,relationship_properties: List[str],relationship_types: Strings = ["*"],separate_property_columns: bool = False,**config: Any,) -> DataFrame

    Streams the given node properties.

.. py:function:: gds.graph.nodeProperties.write(self, G: Graph, node_properties: List[str], node_labels: Strings = ["*"], **config: Any) -> "Series[Any]"

    Writes the given node properties to an online Neo4j database.

.. py:function:: gds.graph.nodeProperty.stream(self, G: Graph, node_properties: str, node_labels: Strings = ["*"], **config: Any) -> DataFrame

    Streams the given node property.

.. py:function:: gds.graph.project(self, graph_name: str, node_spec: Any, relationship_spec: Any, **config: Any) -> Tuple[Graph, "Series[Any]"]

    Creates a named graph in the catalog for use by algorithms.

.. py:function:: gds.graph.project.cypher(self, graph_name: str, node_spec: Any, relationship_spec: Any, **config: Any) -> Tuple[Graph, "Series[Any]"]

    Creates a named graph in the catalog for use by algorithms.

.. py:function:: gds.graph.project.cypher.estimate(self, node_projection: Any, relationship_projection: Any, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.graph.project.estimate(self, node_projection: Any, relationship_projection: Any, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.graph.relationship.write(self, G: Graph, relationship_type: str, relationship_property: str = "", **config: Any) -> "Series[Any]"

    Writes the given relationship and an optional relationship property to an online Neo4j database.

.. py:function:: gds.graph.relationshipProperties.stream(self,G: Graph,relationship_properties: List[str],relationship_types: Strings = ["*"],separate_property_columns: bool = False,**config: Any,) -> DataFrame

    Streams the given relationship properties.

.. py:function:: gds.graph.relationshipProperty.stream(self, G: Graph, node_properties: str, node_labels: Strings = ["*"], **config: Any) -> DataFrame

    Streams the given relationship property.

.. py:function:: gds.graph.relationships.drop(self, G: Graph, relationship_type: str,) -> "Series[Any]"

    Delete the relationship type for a given graph stored in the graph-catalog.

.. py:function:: gds.graph.removeNodeProperties(self, G: Graph, node_properties: List[str], **config: Any,) -> Series

    Removes node properties from a projected graph.

.. py:function:: gds.graph.streamNodeProperties(self, G: Graph, node_properties: List[str], node_labels: Strings = ["*"], separate_property_columns: bool = False, **config: Any) -> DataFrame

    Streams the given node properties.

.. py:function:: gds.graph.streamNodeProperty(self, G: Graph, node_properties: str, node_labels: Strings = ["*"], **config: Any) -> DataFrame

    Streams the given node property.

.. py:function:: gds.graph.streamRelationshipProperties(self, G: Graph, relationship_properties: List[str], relationship_types: Strings = ["*"], separate_property_columns: bool = False, **config: Any) -> DataFrame

    Streams the given relationship properties.

.. py:function:: gds.graph.streamRelationshipProperty(self, G: Graph, relationship_properties: str, relationship_types: Strings = ["*"], **config: Any) -> DataFrame

    Streams the given relationship property.

.. py:function:: gds.graph.writeNodeProperties(self, G: Graph, node_properties: List[str], node_labels: Strings = ["*"], **config: Any) -> "Series[Any]"

    Writes the given node properties to an online Neo4j database.

.. py:function:: gds.graph.writeRelationship(self, G: Graph, relationship_type: str, relationship_property: str = "", **config: Any) -> "Series[Any]"

    Writes the given relationship and an optional relationship property to an online Neo4j database.
