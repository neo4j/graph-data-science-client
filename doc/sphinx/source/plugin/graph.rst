Graph procedures
----------------
Listing of all graph procedures in the Neo4j Graph Data Science Python Client API.
This includes all the methods for projecting, deleting, and listing graphs, as well as methods for handling node or relationship properties.
These all assume that an object of :class:`.GraphDataScience` is available as `gds`.


.. py:function:: gds.graph.construct(graph_name: str, nodes: Union[pandas.DataFrame, List[pandas.DataFrame]], relationships: Optional[Union[pandas.DataFrame, List[pandas.DataFrame]]] = None, concurrency: int = 4, undirected_relationship_types: Optional[List[str]] = None) -> Graph

    Constructs a new graph in the graph catalog, using the provided node and relationship data frames.

.. py:function:: gds.graph.get(graph_name: str) -> Graph

    Gets a graph object representing a graph in the graph catalog.

.. py:function:: gds.alpha.graph.graphProperty.drop(G: Graph, graph_property: str, **config: Any) -> pandas.Series[Any]

    Removes a graph property from a projected graph.

.. py:function:: gds.alpha.graph.graphProperty.stream(G: Graph, graph_property: str, **config: Any) -> pandas.DataFrame

    Streams the given graph property.

.. py:function:: gds.alpha.graph.nodeLabel.mutate(G: Graph, node_label: str, **config: Any) -> pandas.Series[Any]
    Mutates the in-memory graph with the given node Label.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.graph.nodeLabel.mutate` instead.

.. py:function:: gds.alpha.graph.nodeLabel.write(G: Graph, node_label: str, **config: Any) -> pandas.Series[Any]
    Writes the given node Label to an online Neo4j database.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.graph.nodeLabel.write` instead.

.. py:function:: gds.beta.graph.export.csv(G: Graph, **config: Any) -> pandas.Series[Any]

    Exports a named graph to CSV files.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.graph.export.csv` instead.

.. py:function:: gds.beta.graph.export.csv.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    Estimate the required disk space for exporting a named graph to CSV files.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.graph.export.csv.estimate` instead.

.. py:function:: gds.beta.graph.generate(graph_name: str, node_count: int, average_degree: int, **config: Any) -> GraphCreateResult

    Computes a random graph, which will be stored in the graph catalog.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.graph.generate` instead.

.. py:function:: gds.beta.graph.project.subgraph(graph_name: str, from_G: Graph, node_filter: str, relationship_filter: str, **config: Any,) -> GraphCreateResult

    Filters down a graph projection to a named subgraph projection in the catalog for use by algorithms.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.graph.filter` instead.

.. py:function:: gds.beta.graph.relationships.stream(G: Graph, relationship_types: List[str] = ["*"], **config: Any) -> Topologypandas.DataFrame

    Streams the given relationship source/target pairs

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.graph.relationships.stream` instead.

.. py:function:: gds.beta.graph.relationships.toUndirected(G: Graph, query: str, relationship_type: str, mutate_relationship_type: str, **config: Any) -> pandas.Series[Any]

    The ToUndirected procedure converts directed relationships to undirected relationships

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.graph.relationships.toUndirected` instead.

.. py:function:: gds.beta.graph.relationships.toUndirected.estimate(G: Graph, relationship_type: str, mutate_relationship_type: str, **config: Any) -> pandas.Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.graph.relationships.toUndirected.estimate` instead.

.. py:function:: gds.graph.cypher.project(query: str, database: Optional[str], **params: Any) -> GraphCreateResult

    Use Cypher projection to creates a named graph in the catalog for use by algorithms.
    The provided query must end with a `RETURN gds.graph.project(...)` call.

.. py:function:: gds.graph.deleteRelationships(G: Graph, relationship_type: str) -> pandas.Series[Any]

    Delete the relationship type for a given graph stored in the graph-catalog.

.. py:function:: gds.graph.drop(G: Union[Graph | str], failIfMissing: bool = False,dbName: str = "",username: Optional[str] = None,) -> Optional[pandas.Series[Any]]

    Drops a named graph from the catalog and frees up the resources it occupies.

.. py:function:: gds.graph.exists(graph_name: str) -> pandas.Series[Any]

    Checks if a graph exists in the catalog.

.. py:function:: gds.graph.export(G: Graph, **config: Any) -> pandas.Series[Any]

    Exports a named graph into a new offline Neo4j database.

.. py:function:: gds.graph.export.csv(G: Graph, **config: Any) -> pandas.Series[Any]

    Exports a named graph to CSV files.

.. py:function:: gds.graph.export.csv.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    Estimate the required disk space for exporting a named graph to CSV files.

.. py:function:: gds.graph.generate(graph_name: str, node_count: int, average_degree: int, **config: Any) -> GraphCreateResult

    Computes a random graph, which will be stored in the graph catalog.

.. py:function:: gds.graph.graphProperty.drop(G: Graph, graph_property: str, **config: Any) -> pandas.Series[Any]

    Removes a graph property from a projected graph.

.. py:function:: gds.graph.graphProperty.stream(G: Graph, graph_property: str, **config: Any) -> pandas.DataFrame

    Streams the given graph property.

.. py:function:: gds.graph.list(G: Optional[Union[Graph, str]] = None) -> pandas.DataFrame

    Lists information about named graphs stored in the catalog.

.. py:function:: gds.graph.nodeProperties.drop(G: Graph, node_properties: List[str], **config: Any) -> pandas.Series[Any]

    Removes node properties from a projected graph.

.. py:function:: gds.graph.nodeProperties.stream(G: Graph,node_properties: Union[List[str], str],node_labels: Strings = ["*"],separate_property_columns: bool = False, db_node_properties: List[str] = [], **config: Any,) -> pandas.DataFrame

    Streams the given node properties.

.. py:function:: gds.graph.nodeProperties.write(G: Graph, node_properties: List[str], node_labels: Strings = ["*"], **config: Any) -> pandas.Series[Any]

    Writes the given node properties to an online Neo4j database.

.. py:function:: gds.graph.nodeProperty.stream(G: Graph, node_properties: str, node_labels: Strings = ["*"], db_node_properties: List[str] = [], **config: Any) -> pandas.DataFrame

    Streams the given node property.

.. py:function:: gds.graph.project(graph_name: str, node_spec: Any, relationship_spec: Any, **config: Any) -> GraphCreateResult

    Creates a named graph in the catalog for use by algorithms.

.. py:function:: gds.graph.project.cypher(graph_name: str, node_spec: Any, relationship_spec: Any, **config: Any) -> GraphCreateResult

    Creates a named graph in the catalog for use by algorithms.

.. py:function:: gds.graph.project.cypher.estimate(node_projection: Any, relationship_projection: Any, **config: Any) -> pandas.Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.graph.project.estimate(node_projection: Any, relationship_projection: Any, **config: Any) -> pandas.Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.graph.filter` instead.

.. py:function:: gds.graph.relationships.stream(G: Graph, relationship_types: List[str] = ["*"], **config: Any) -> Topologypandas.DataFrame

    Streams the given relationship source/target pairs

.. py:function:: gds.graph.relationships.toUndirected(G: Graph, query: str, relationship_type: str, mutate_relationship_type: str, **config: Any) -> pandas.Series[Any]

    The ToUndirected procedure converts directed relationships to undirected relationships

.. py:function:: gds.graph.relationships.toUndirected.estimate(G: Graph, relationship_type: str, mutate_relationship_type: str, **config: Any) -> pandas.Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.graph.relationship.write(G: Graph, relationship_type: str, relationship_property: str = "", **config: Any) -> pandas.Series[Any]

    Writes the given relationship and an optional relationship property to an online Neo4j database.

.. py:function:: gds.graph.relationshipProperties.stream(G: Graph, relationship_properties: List[str],relationship_types: Union[str, List[str]] = ["*"],separate_property_columns: bool = False,**config: Any,) -> pandas.DataFrame

    Streams the given relationship properties.

.. py:function:: gds.graph.relationshipProperties.write(G: Graph, relationship_type: str, relationship_properties: List[str], **config: Any,) -> pandas.DataFrame

    Write the given relationship properties back to the database.

.. py:function:: gds.graph.relationshipProperty.stream(G: Graph, node_properties: str, node_labels: Union[str, List[str]] = ["*"], **config: Any) -> pandas.DataFrame

    Streams the given relationship property.

.. py:function:: gds.graph.relationships.drop(G: Graph, relationship_type: str,) -> pandas.Series[Any]

    Delete the relationship type for a given graph stored in the graph-catalog.

.. py:function:: gds.graph.removeNodeProperties(G: Graph, node_properties: List[str], **config: Any,) -> Series

    Removes node properties from a projected graph.

.. py:function:: gds.graph.streamNodeProperties(G: Graph, node_properties: List[str], node_labels: Strings = ["*"], separate_property_columns: bool = False, **config: Any) -> pandas.DataFrame

    Streams the given node properties.

.. py:function:: gds.graph.streamNodeProperty(G: Graph, node_properties: str, node_labels: Strings = ["*"], **config: Any) -> pandas.DataFrame

    Streams the given node property.

.. py:function:: gds.graph.streamRelationshipProperties(G: Graph, relationship_properties: List[str], relationship_types: Strings = ["*"], separate_property_columns: bool = False, **config: Any) -> pandas.DataFrame

    Streams the given relationship properties.

.. py:function:: gds.graph.streamRelationshipProperty(G: Graph, relationship_properties: str, relationship_types: Strings = ["*"], **config: Any) -> pandas.DataFrame

    Streams the given relationship property.

.. py:function:: gds.graph.writeNodeProperties(G: Graph, node_properties: List[str], node_labels: Strings = ["*"], **config: Any) -> pandas.Series[Any]

    Writes the given node properties to an online Neo4j database.

.. py:function:: gds.graph.writeRelationship(G: Graph, relationship_type: str, relationship_property: str = "", **config: Any) -> pandas.Series[Any]

    Writes the given relationship and an optional relationship property to an online Neo4j database.

.. py:function:: gds.graph.load_cora(graph_name: str = "cora", undirected: bool = False) -> Graph

    Loads the Cora dataset into a named graph in the catalog for use by algorithms.

.. py:function:: gds.graph.load_karate_club(graph_name: str = "karate_club", undirected: bool = False) -> Graph

    Loads the Karate Club dataset into a named graph in the catalog for use by algorithms.

.. py:function:: gds.graph.load_imdb(graph_name: str = "imdb", undirected: bool = True) -> Graph

    Loads the IMDB dataset into a named graph in the catalog for use by algorithms.

.. py:function:: gds.graph.load_lastfm(graph_name: str = "lastfm", undirected: bool = True) -> Graph

    Loads the LastFM dataset into a named graph in the catalog for use by algorithms.

.. py:function:: gds.graph.ogbn.load(dataset_name: str, dataset_root_path: str = "dataset", graph_name: Optional[str] = None, concurrency: int = 4) -> Graph

    Loads a OGBN dataset into a named graph in the catalog for use by algorithms.

.. py:function:: gds.graph.ogbl.load(dataset_name: str, dataset_root_path: str = "dataset", graph_name: Optional[str] = None, concurrency: int = 4) -> Graph

    Loads a OGBL dataset into a named graph in the catalog for use by algorithms.

.. py:function:: gds.graph.networkx.load(nx_G: networkx.Graph, graph_name: str, concurrency: int = 4) -> Graph

    Loads a NetworkX graph into a named graph in the catalog for use by algorithms.

.. py:function:: gds.find_node_id(labels: List[str] = [], properties: Dict[str, Any] = {}) -> int

    Finds a node id by its labels and properties.

.. py:function:: gds.graph.nodeLabel.mutate(G: Graph, node_label: str, **config: Any) -> pandas.Series[Any]
    Mutates the in-memory graph with the given node Label.

.. py:function:: gds.graph.nodeLabel.write(G: Graph, node_label: str, **config: Any) -> pandas.Series[Any]
    Writes the given node Label to an online Neo4j database.

.. py:function:: gds.graph.filter(graph_name: str, from_G: Graph, node_filter: str, relationship_filter: str, **config: Any,) -> GraphCreateResult

    Filters down a graph projection to a named subgraph projection in the catalog for use by algorithms.
