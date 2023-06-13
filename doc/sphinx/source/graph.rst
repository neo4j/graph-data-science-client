Graph procedures
----------------
Listing of all graph procedures in the Neo4j Graph Data Science Python Client API.
This includes all the methods for projecting, deleting, and listing graphs, as well as methods for handling node or relationship properties.
These all assume that an object of :class:`.GraphDataScience` is available as `gds`.


.. py:function:: gds.graph.construct(graph_name: str, nodes: Union[DataFrame, List[DataFrame]], relationships: Union[DataFrame, List[DataFrame]], concurrency: int = 4, undirected_relationship_types: Optional[List[str]] = None) -> Graph

    Constructs a new graph in the graph catalog, using the provided node and relationship data frames.

.. py:function:: gds.graph.get(graph_name: str) -> Graph

    Gets a graph object representing a graph in the graph catalog.

.. py:function:: gds.alpha.graph.graphProperty.drop(G: Graph, graph_property: str, **config: Any) -> Series[Any]

    Removes a graph property from a projected graph.

.. py:function:: gds.alpha.graph.graphProperty.stream(G: Graph, graph_property: str, **config: Any) -> DataFrame

    Streams the given graph property.

.. py:function:: gds.alpha.graph.nodeLabel.mutate(G: Graph, node_label: str, **config: Any) -> Series[Any]

    Mutates the in-memory graph with the given node Label.

.. py:function:: gds.alpha.graph.nodeLabel.write(G: Graph, node_label: str, **config: Any) -> Series[Any]

    Writes the given node Label to an online Neo4j database.

.. py:function:: gds.beta.graph.export.csv(G: Graph, **config: Any) -> Series[Any]

    Exports a named graph to CSV files.

.. py:function:: gds.beta.graph.export.csv.estimate(G: Graph, **config: Any) -> "Series[Any]

    Estimate the required disk space for exporting a named graph to CSV files.

.. py:function:: gds.beta.graph.generate(graph_name: str, node_count: int, average_degree: int, **config: Any) -> Tuple[Graph, Series[Any]]

    Computes a random graph, which will be stored in the graph catalog.

.. py:function:: gds.beta.graph.project.subgraph(graph_name: str,from_G: Graph,node_filter: str,relationship_filter: str,**config: Any,) -> Tuple[Graph, Series[Any]]

    Creates a named graph in the catalog for use by algorithms.

.. py:function:: gds.beta.graph.relationships.stream(G: Graph, relationship_types: List[str] = ["*"], **config: Any) -> TopologyDataFrame

    Streams the given relationship source/target pairs

.. py:function:: gds.beta.graph.relationships.toUndirected(G: Graph, query: str, relationship_type: str, mutate_relationship_type: str, **config: Any) -> Series[Any]

    The ToUndirected procedure converts directed relationships to undirected relationships

.. py:function:: gds.beta.graph.relationships.toUndirected.estimate(G: Graph, relationship_type: str, mutate_relationship_type: str, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.graph.deleteRelationships(G: Graph, relationship_type: str) -> Series[Any]

    Delete the relationship type for a given graph stored in the graph-catalog.

.. py:function:: gds.graph.drop(G: Graph,failIfMissing: bool = False,dbName: str = "",username: Optional[str] = None,) -> Optional[Series[Any]]

    Drops a named graph from the catalog and frees up the resources it occupies.

.. py:function:: gds.graph.exists(graph_name: str) -> Series[Any]

    Checks if a graph exists in the catalog.

.. py:function:: gds.graph.export(G: Graph, **config: Any) -> Series[Any]

    Exports a named graph into a new offline Neo4j database.

.. py:function:: gds.graph.list(G: Optional[Graph] = None) -> DataFrame

    Lists information about named graphs stored in the catalog.

.. py:function:: gds.graph.nodeProperties.drop(G: Graph, node_properties: List[str], **config: Any) -> Series[Any]

    Removes node properties from a projected graph.

.. py:function:: gds.graph.nodeProperties.stream(G: Graph,relationship_properties: List[str],relationship_types: Strings = ["*"],separate_property_columns: bool = False,**config: Any,) -> DataFrame

    Streams the given node properties.

.. py:function:: gds.graph.nodeProperties.write(G: Graph, node_properties: List[str], node_labels: Strings = ["*"], **config: Any) -> Series[Any]

    Writes the given node properties to an online Neo4j database.

.. py:function:: gds.graph.nodeProperty.stream(G: Graph, node_properties: str, node_labels: Strings = ["*"], **config: Any) -> DataFrame

    Streams the given node property.

.. py:function:: gds.graph.project(graph_name: str, node_spec: Any, relationship_spec: Any, **config: Any) -> Tuple[Graph, Series[Any]]

    Creates a named graph in the catalog for use by algorithms.

.. py:function:: gds.graph.project.cypher(graph_name: str, node_spec: Any, relationship_spec: Any, **config: Any) -> Tuple[Graph, Series[Any]]

    Creates a named graph in the catalog for use by algorithms.

.. py:function:: gds.graph.project.cypher.estimate(node_projection: Any, relationship_projection: Any, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.graph.project.estimate(node_projection: Any, relationship_projection: Any, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.graph.relationship.write(G: Graph, relationship_type: str, relationship_property: str = "", **config: Any) -> Series[Any]

    Writes the given relationship and an optional relationship property to an online Neo4j database.

.. py:function:: gds.graph.relationshipProperties.stream(G: Graph, relationship_properties: List[str],relationship_types: Union[str, List[str]] = ["*"],separate_property_columns: bool = False,**config: Any,) -> DataFrame

    Streams the given relationship properties.

.. py:function:: gds.graph.relationshipProperties.write(G: Graph, relationship_type: str, relationship_properties: List[str], **config: Any,) -> DataFrame

    Write the given relationship properties back to the database.

.. py:function:: gds.graph.relationshipProperty.stream(G: Graph, node_properties: str, node_labels: Union[str, List[str]] = ["*"], **config: Any) -> DataFrame

    Streams the given relationship property.

.. py:function:: gds.graph.relationships.drop(G: Graph, relationship_type: str,) -> Series[Any]

    Delete the relationship type for a given graph stored in the graph-catalog.

.. py:function:: gds.graph.removeNodeProperties(G: Graph, node_properties: List[str], **config: Any,) -> Series

    Removes node properties from a projected graph.

.. py:function:: gds.graph.streamNodeProperties(G: Graph, node_properties: List[str], node_labels: Strings = ["*"], separate_property_columns: bool = False, **config: Any) -> DataFrame

    Streams the given node properties.

.. py:function:: gds.graph.streamNodeProperty(G: Graph, node_properties: str, node_labels: Strings = ["*"], **config: Any) -> DataFrame

    Streams the given node property.

.. py:function:: gds.graph.streamRelationshipProperties(G: Graph, relationship_properties: List[str], relationship_types: Strings = ["*"], separate_property_columns: bool = False, **config: Any) -> DataFrame

    Streams the given relationship properties.

.. py:function:: gds.graph.streamRelationshipProperty(G: Graph, relationship_properties: str, relationship_types: Strings = ["*"], **config: Any) -> DataFrame

    Streams the given relationship property.

.. py:function:: gds.graph.writeNodeProperties(G: Graph, node_properties: List[str], node_labels: Strings = ["*"], **config: Any) -> Series[Any]

    Writes the given node properties to an online Neo4j database.

.. py:function:: gds.graph.writeRelationship(G: Graph, relationship_type: str, relationship_property: str = "", **config: Any) -> Series[Any]

    Writes the given relationship and an optional relationship property to an online Neo4j database.

.. py:function:: gds.graph.load_cora(graph_name: str = "cora", undirected: bool = False) -> Graph

    Loads the Cora dataset into a named graph in the catalog for use by algorithms.

.. py:function:: gds.graph.load_karate_club(graph_name: str = "karate_club", undirected: bool = False) -> Graph

    Loads the Karate Club dataset into a named graph in the catalog for use by algorithms.

.. py:function:: gds.graph.load_imdb(graph_name: str = "imdb", undirected: bool = True) -> Graph

    Loads the IMDB dataset into a named graph in the catalog for use by algorithms.

.. py:function:: gds.graph.ogbn.load(dataset_name: str, dataset_root_path: str = "dataset", graph_name: Optional[str] = None, concurrency: int = 4) -> Graph

    Loads a OGBN dataset into a named graph in the catalog for use by algorithms.

.. py:function:: gds.graph.ogbl.load(dataset_name: str, dataset_root_path: str = "dataset", graph_name: Optional[str] = None, concurrency: int = 4) -> Graph

    Loads a OGBL dataset into a named graph in the catalog for use by algorithms.

.. py:function:: gds.graph.networkx.load(nx_G: nx.Graph, graph_name: str, concurrency: int = 4) -> Graph

    Loads a NetworkX graph into a named graph in the catalog for use by algorithms.

.. py:function:: gds.find_node_id(labels: List[str] = [], properties: Dict[str, Any] = {}) -> int

    Finds a node id by its labels and properties.
