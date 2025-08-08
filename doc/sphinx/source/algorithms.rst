..
    DO NOT EDIT - File generated automatically

Algorithms procedures
----------------------
Listing of all algorithm procedures in the Neo4j Graph Data Science Python Client API.
These all assume that an object of :class:`.GraphDataScience` is available as `gds`.

.. py:function:: gds.allShortestPaths.delta.mutate(G: Graph, **config: Any) -> Series[Any]

    The Delta Stepping shortest path algorithm computes the shortest (weighted) path
    between one node and any other node in the graph. The computation is run multi-threaded.

.. py:function:: gds.allShortestPaths.delta.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for allShortestPaths.dealta.mutate.

.. py:function:: gds.allShortestPaths.delta.stats(G: Graph, **config: Any) -> Series[Any]

    The Delta Stepping shortest path algorithm computes the shortest (weighted) path
    between one node and any other node in the graph. The computation is run multi-threaded

.. py:function:: gds.allShortestPaths.delta.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for allShortestPaths.delta.stats.

.. py:function:: gds.allShortestPaths.delta.stream(G: Graph, **config: Any) -> DataFrame

    The Delta Stepping shortest path algorithm computes the shortest (weighted) path
    between one node and any other node in the graph. The computation is run multi-threaded.

.. py:function:: gds.allShortestPaths.delta.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for allShortestPaths.delta.strema.

.. py:function:: gds.allShortestPaths.delta.write(G: Graph, **config: Any) -> Series[Any]

    The Delta Stepping shortest path algorithm computes the shortest (weighted) path
    between one node and any other node in the graph. The computation is run multi-threaded.

.. py:function:: gds.allShortestPaths.delta.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.allShortestPaths.dijkstra.mutate(G: Graph, **config: Any) -> Series[Any]

    The Dijkstra shortest path algorithm computes the shortest (weighted) path
    between one node and any other node in the graph.

.. py:function:: gds.allShortestPaths.dijkstra.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.allShortestPaths.dijkstra.stream(G: Graph, **config: Any) -> DataFrame

    The Dijkstra shortest path algorithm computes the shortest (weighted) path
    between one node and any other node in the graph.

.. py:function:: gds.allShortestPaths.dijkstra.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.allShortestPaths.dijkstra.write(G: Graph, **config: Any) -> Series[Any]

    The Dijkstra shortest path algorithm computes the shortest (weighted) path
    between one node and any other node in the graph.

.. py:function:: gds.allShortestPaths.dijkstra.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.allShortestPaths.stream(G: Graph, **config: Any) -> DataFrame

    The All Pairs Shortest Path (APSP) calculates the shortest (weighted) path
    between all pairs of nodes.

.. py:function:: gds.allShortestPaths.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.allShortestPaths.stream(G: Graph, **config: Any) -> DataFrame

    The All Pairs Shortest Path (APSP) calculates the shortest (weighted) path
    between all pairs of nodes.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.allShortestPaths.stream` instead.

.. py:function:: gds.alpha.closeness.harmonic.stream(G: Graph, **config: Any) -> DataFrame

    Harmonic centrality is a way of detecting nodes that are able to spread information
    very efficiently through a graph.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.closeness.harmonic.stream` instead.

.. py:function:: gds.alpha.closeness.harmonic.write(G: Graph, **config: Any) -> Series[Any]

    Harmonic centrality is a way of detecting nodes that are able to spread information
    very efficiently through a graph.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.closeness.harmonic.write` instead.

.. py:function:: gds.alpha.conductance.stream(G: Graph, **config: Any) -> DataFrame

    Evaluates a division of nodes into communities based on the proportion of relationships
    that cross community boundaries.

.. py:function:: gds.alpha.graph.sample.rwr(graph_name: str, from_G: Graph, **config: Any) -> GraphCreateResult

    Constructs a random subgraph based on random walks with restarts.

.. deprecated:: 2.4.0
   Since GDS server version 2.4.0 you should use the endpoint :func:`gds.graph.sample.rwr` instead.

.. py:function:: gds.alpha.hits.mutate(G: Graph, **config: Any) -> Series[Any]

    Hyperlink-Induced Topic Search (HITS) is a link analysis algorithm that rates nodes.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.hits.mutate` instead.

.. py:function:: gds.alpha.hits.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.hits.mutate.estimate` instead.

.. py:function:: gds.alpha.hits.stats(G: Graph, **config: Any) -> Series[Any]

    Hyperlink-Induced Topic Search (HITS) is a link analysis algorithm that rates nodes.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.hits.stats` instead.

.. py:function:: gds.alpha.hits.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.hits.stats.estimate` instead.

.. py:function:: gds.alpha.hits.stream(G: Graph, **config: Any) -> DataFrame

    Hyperlink-Induced Topic Search (HITS) is a link analysis algorithm that rates nodes.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.hits.stream` instead.

.. py:function:: gds.alpha.hits.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.hits.stream.estimate` instead.

.. py:function:: gds.alpha.hits.write(G: Graph, **config: Any) -> Series[Any]

    Hyperlink-Induced Topic Search (HITS) is a link analysis algorithm that rates nodes.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.hits.write` instead.

.. py:function:: gds.alpha.hits.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.hits.write.estimate` instead.

.. py:function:: gds.alpha.kSpanningTree.write(G: Graph, **config: Any) -> Series[Any]

    The K-spanning tree algorithm starts from a root node and returns a spanning tree with exactly k nodes

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.kSpanningTree.write` instead.

.. py:function:: gds.alpha.knn.filtered.mutate(G: Graph, **config: Any) -> Series[Any]

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties.
    Filtered KNN extends this functionality, allowing filtering on source nodes and target nodes, respectively.

.. py:function:: gds.alpha.knn.filtered.stats(G: Graph, **config: Any) -> Series[Any]

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties.
    Filtered KNN extends this functionality, allowing filtering on source nodes and target nodes, respectively.

.. py:function:: gds.alpha.knn.filtered.stream(G: Graph, **config: Any) -> DataFrame

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties.
    Filtered KNN extends this functionality, allowing filtering on source nodes and target nodes, respectively.

.. py:function:: gds.alpha.knn.filtered.write(G: Graph, **config: Any) -> Series[Any]

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties.
    Filtered KNN extends this functionality, allowing filtering on source nodes and target nodes, respectively.

.. py:function:: gds.alpha.linkprediction.adamicAdar(node1: int, node2: int, **config: Any) -> float

    Given two nodes, calculate Adamic Adar similarity

.. py:function:: gds.alpha.linkprediction.commonNeighbors(node1: int, node2: int, **config: Any) -> float

    Given two nodes, returns the number of common neighbors

.. py:function:: gds.alpha.linkprediction.preferentialAttachment(node1: int, node2: int, **config: Any) -> float

    Given two nodes, calculate Preferential Attachment

.. py:function:: gds.alpha.linkprediction.resourceAllocation(node1: int, node2: int, **config: Any) -> float

    Given two nodes, calculate Resource Allocation similarity

.. py:function:: gds.alpha.linkprediction.sameCommunity(node1: int, node2: int, communityProperty: Optional[str] = None) -> float

    Given two nodes, indicates if they have the same community

.. py:function:: gds.alpha.linkprediction.totalNeighbors(node1: int, node2: int, **config: Any) -> float

    Given two nodes, calculate Total Neighbors

.. py:function:: gds.alpha.maxkcut.mutate(G: Graph, **config: Any) -> Series[Any]

    Approximate Maximum k-cut maps each node into one of k disjoint communities
    trying to maximize the sum of weights of relationships between these communities.

.. py:function:: gds.alpha.maxkcut.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Approximate Maximum k-cut maps each node into one of k disjoint communities
    trying to maximize the sum of weights of relationships between these communities.

.. py:function:: gds.alpha.maxkcut.stream(G: Graph, **config: Any) -> DataFrame

    Approximate Maximum k-cut maps each node into one of k disjoint communities
    trying to maximize the sum of weights of relationships between these communities.

.. py:function:: gds.alpha.maxkcut.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Approximate Maximum k-cut maps each node into one of k disjoint communities
    trying to maximize the sum of weights of relationships between these communities.

.. py:function:: gds.alpha.modularity.stats(G: Graph, **config: Any) -> Series[Any]

.. py:function:: gds.alpha.modularity.stream(G: Graph, **config: Any) -> DataFrame

.. py:function:: gds.alpha.nodeSimilarity.filtered.mutate(G: Graph, **config: Any) -> Series[Any]

    The Filtered Node Similarity algorithm compares a set of nodes based on the nodes they are connected to.
    Two nodes are considered similar if they share many of the same neighbors.
    The algorithm computes pair-wise similarities based on Jaccard or Overlap metrics.
    The filtered variant supports limiting which nodes to compare via source and target node filters.

.. py:function:: gds.alpha.nodeSimilarity.filtered.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.nodeSimilarity.filtered.stats(G: Graph, **config: Any) -> Series[Any]

    The Filtered Node Similarity algorithm compares a set of nodes based on the nodes they are connected to.
    Two nodes are considered similar if they share many of the same neighbors.
    The algorithm computes pair-wise similarities based on Jaccard or Overlap metrics.
    The filtered variant supports limiting which nodes to compare via source and target node filters.

.. py:function:: gds.alpha.nodeSimilarity.filtered.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.nodeSimilarity.filtered.stream(G: Graph, **config: Any) -> DataFrame

    The Filtered Node Similarity algorithm compares a set of nodes based on the nodes they are connected to.
    Two nodes are considered similar if they share many of the same neighbors.
    The algorithm computes pair-wise similarities based on Jaccard or Overlap metrics.
    The filtered variant supports limiting which nodes to compare via source and target node filters.

.. py:function:: gds.alpha.nodeSimilarity.filtered.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.nodeSimilarity.filtered.write(G: Graph, **config: Any) -> Series[Any]

    The Filtered Node Similarity algorithm compares a set of nodes based on the nodes they are connected to.
    Two nodes are considered similar if they share many of the same neighbors.
    The algorithm computes pair-wise similarities based on Jaccard or Overlap metrics.
    The filtered variant supports limiting which nodes to compare via source and target node filters.

.. py:function:: gds.alpha.nodeSimilarity.filtered.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.scaleProperties.mutate(G: Graph, **config: Any) -> Series[Any]

    Scale node properties

.. deprecated:: 2.4.0
   Since GDS server version 2.4.0 you should use the endpoint :func:`gds.scaleProperties.mutate` instead.

.. py:function:: gds.alpha.scaleProperties.stream(G: Graph, **config: Any) -> DataFrame

    Scale node properties

.. deprecated:: 2.4.0
   Since GDS server version 2.4.0 you should use the endpoint :func:`gds.scaleProperties.stream` instead.

.. py:function:: gds.alpha.scc.stream(G: Graph, **config: Any) -> DataFrame

    The SCC algorithm finds sets of connected nodes in an directed graph,
    where all nodes in the same set form a connected component.

.. py:function:: gds.alpha.scc.write(G: Graph, **config: Any) -> Series[Any]

    The SCC algorithm finds sets of connected nodes in an directed graph,
    where all nodes in the same set form a connected component.

.. py:function:: gds.alpha.sllpa.mutate(G: Graph, **config: Any) -> Series[Any]

    The Speaker Listener Label Propagation algorithm is a fast algorithm for finding overlapping communities in a graph.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.sllpa.mutate` instead.

.. py:function:: gds.alpha.sllpa.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.sllpa.mutate.estimate` instead.

.. py:function:: gds.alpha.sllpa.stats(G: Graph, **config: Any) -> Series[Any]

    The Speaker Listener Label Propagation algorithm is a fast algorithm for finding overlapping communities in a graph.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.sllpa.stats` instead.

.. py:function:: gds.alpha.sllpa.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.sllpa.stats.estimate` instead.

.. py:function:: gds.alpha.sllpa.stream(G: Graph, **config: Any) -> DataFrame

    The Speaker Listener Label Propagation algorithm is a fast algorithm for finding overlapping communities in a graph.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.sllpa.stream` instead.

.. py:function:: gds.alpha.sllpa.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.sllpa.stream.estimate` instead.

.. py:function:: gds.alpha.sllpa.write(G: Graph, **config: Any) -> Series[Any]

    The Speaker Listener Label Propagation algorithm is a fast algorithm for finding overlapping communities in a graph.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.sllpa.write` instead.

.. py:function:: gds.alpha.sllpa.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.sllpa.write.estimate` instead.

.. py:function:: gds.alpha.triangles(G: Graph, **config: Any) -> DataFrame

    Triangles streams the nodeIds of each triangle in the graph.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.triangles` instead.

.. py:function:: gds.articleRank.mutate(G: Graph, **config: Any) -> Series[Any]

    Article Rank is a variant of the Page Rank algorithm, which measures the transitive influence or connectivity of nodes.

.. py:function:: gds.articleRank.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.articleRank.stats(G: Graph, **config: Any) -> Series[Any]

    Executes the algorithm and returns result statistics without writing the result to Neo4j.

.. py:function:: gds.articleRank.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.articleRank.stream(G: Graph, **config: Any) -> DataFrame

    Article Rank is a variant of the Page Rank algorithm, which measures the transitive influence or connectivity of nodes.

.. py:function:: gds.articleRank.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.articleRank.write(G: Graph, **config: Any) -> Series[Any]

    Article Rank is a variant of the Page Rank algorithm, which measures the transitive influence or connectivity of nodes.

.. py:function:: gds.articleRank.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.articulationPoints.mutate(G: Graph, **config: Any) -> Series[Any]

    Articulation Points is an algorithm that finds nodes that disconnect components if removed.

.. py:function:: gds.articulationPoints.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.articulationPoints.stats(G: Graph, **config: Any) -> Series[Any]

    Articulation Points is an algorithm that finds nodes that disconnect components if removed.

.. py:function:: gds.articulationPoints.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.articulationPoints.stream(G: Graph, **config: Any) -> Series[Any]

    Articulation Points is an algorithm that finds nodes that disconnect components if removed.

.. py:function:: gds.articulationPoints.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.articulationPoints.write(G: Graph, **config: Any) -> Series[Any]

    Articulation Points is an algorithm that finds nodes that disconnect components if removed.

.. py:function:: gds.articulationPoints.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.bellmanFord.mutate(G: Graph, **config: Any) -> Series[Any]

    The Bellman-Ford shortest path algorithm computes the shortest (weighted) path between one node
    and any other node in the graph without negative cycles.

.. py:function:: gds.bellmanFord.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.bellmanFord.stats(G: Graph, **config: Any) -> Series[Any]

    The Bellman-Ford shortest path algorithm computes the shortest (weighted) path between one node
    and any other node in the graph without negative cycles.

.. py:function:: gds.bellmanFord.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.bellmanFord.stream(G: Graph, **config: Any) -> DataFrame

    The Bellman-Ford shortest path algorithm computes the shortest (weighted) path between one node
    and any other node in the graph without negative cycles.

.. py:function:: gds.bellmanFord.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.bellmanFord.write(G: Graph, **config: Any) -> Series[Any]

    The Bellman-Ford shortest path algorithm computes the shortest (weighted) path between one node
    and any other node in the graph without negative cycles.

.. py:function:: gds.bellmanFord.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.closeness.mutate(G: Graph, **config: Any) -> Series[Any]

    Closeness centrality is a way of detecting nodes that are able to spread information very efficiently through a graph.

.. py:function:: gds.beta.closeness.stats(G: Graph, **config: Any) -> Series[Any]

    Closeness centrality is a way of detecting nodes that are able to spread information very efficiently through a graph.

.. py:function:: gds.beta.closeness.stream(G: Graph, **config: Any) -> DataFrame

    Closeness centrality is a way of detecting nodes that are able to spread information very efficiently through a graph.

.. py:function:: gds.beta.closeness.write(G: Graph, **config: Any) -> Series[Any]

    Closeness centrality is a way of detecting nodes that are able to spread information very efficiently through a graph.

.. py:function:: gds.beta.collapsePath.mutate(G: Graph, **config: Any) -> Series[Any]

    Collapse Path algorithm is a traversal algorithm capable of creating relationships between the start
    and end nodes of a traversal

.. py:function:: gds.beta.influenceMaximization.celf.mutate(G: Graph, **config: Any) -> Series[Any]

    The Cost Effective Lazy Forward (CELF) algorithm aims to find k nodes
    that maximize the expected spread of influence in the network.

.. py:function:: gds.beta.influenceMaximization.celf.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.influenceMaximization.celf.stats(G: Graph, **config: Any) -> Series[Any]

    Executes the algorithm and returns result statistics without writing the result to Neo4j.

.. py:function:: gds.beta.influenceMaximization.celf.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.influenceMaximization.celf.stream(G: Graph, **config: Any) -> DataFrame

    The Cost Effective Lazy Forward (CELF) algorithm aims to find k nodes
    that maximize the expected spread of influence in the network.

.. py:function:: gds.beta.influenceMaximization.celf.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    The Cost Effective Lazy Forward (CELF) algorithm aims to find k nodes
    that maximize the expected spread of influence in the network.

.. py:function:: gds.beta.influenceMaximization.celf.write(G: Graph, **config: Any) -> Series[Any]

    The Cost Effective Lazy Forward (CELF) algorithm aims to find k nodes
    that maximize the expected spread of influence in the network.

.. py:function:: gds.beta.influenceMaximization.celf.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.k1coloring.mutate(G: Graph, **config: Any) -> Series[Any]

    The K-1 Coloring algorithm assigns a color to every node in the graph.

.. py:function:: gds.beta.k1coloring.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.k1coloring.stats(G: Graph, **config: Any) -> Series[Any]

    The K-1 Coloring algorithm assigns a color to every node in the graph.

.. py:function:: gds.beta.k1coloring.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.k1coloring.stream(G: Graph, **config: Any) -> DataFrame

    The K-1 Coloring algorithm assigns a color to every node in the graph.

.. py:function:: gds.beta.k1coloring.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.k1coloring.write(G: Graph, **config: Any) -> Series[Any]

    The K-1 Coloring algorithm assigns a color to every node in the graph.

.. py:function:: gds.beta.k1coloring.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.kmeans.mutate(G: Graph, **config: Any) -> Series[Any]

    The Kmeans  algorithm clusters nodes into different communities based on Euclidean distance

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.kmeans.mutate` instead.

.. py:function:: gds.beta.kmeans.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.kmeans.mutate.estimate` instead.

.. py:function:: gds.beta.kmeans.stats(G: Graph, **config: Any) -> Series[Any]

    The Kmeans  algorithm clusters nodes into different communities based on Euclidean distance

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.kmeans.stats` instead.

.. py:function:: gds.beta.kmeans.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.kmeans.stats.estimate` instead.

.. py:function:: gds.beta.kmeans.stream(G: Graph, **config: Any) -> DataFrame

    The Kmeans  algorithm clusters nodes into different communities based on Euclidean distance

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.kmeans.stream` instead.

.. py:function:: gds.beta.kmeans.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.kmeans.stream.estimate` instead.

.. py:function:: gds.beta.kmeans.write(G: Graph, **config: Any) -> Series[Any]

    The Kmeans  algorithm clusters nodes into different communities based on Euclidean distance

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.kmeans.write` instead.

.. py:function:: gds.beta.kmeans.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.kmeans.write.estimate` instead.

.. py:function:: gds.beta.leiden.mutate(G: Graph, **config: Any) -> Series[Any]

    Leiden is a community detection algorithm, which guarantees that communities are well connected

.. py:function:: gds.beta.leiden.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.leiden.stats(G: Graph, **config: Any) -> Series[Any]

    Executes the algorithm and returns result statistics without writing the result to Neo4j.

.. py:function:: gds.beta.leiden.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.leiden.stream(G: Graph, **config: Any) -> DataFrame

    Leiden is a community detection algorithm, which guarantees that communities are well connected

.. py:function:: gds.beta.leiden.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.leiden.write(G: Graph, **config: Any) -> Series[Any]

    Leiden is a community detection algorithm, which guarantees that communities are well connected

.. py:function:: gds.beta.leiden.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.modularityOptimization.mutate(G: Graph, **config: Any) -> Series[Any]

    The Modularity Optimization algorithm groups the nodes in the graph by optimizing the graphs modularity.

.. py:function:: gds.beta.modularityOptimization.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.modularityOptimization.stream(G: Graph, **config: Any) -> DataFrame

    The Modularity Optimization algorithm groups the nodes in the graph by optimizing the graphs modularity.

.. py:function:: gds.beta.modularityOptimization.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.modularityOptimization.write(G: Graph, **config: Any) -> Series[Any]

    The Modularity Optimization algorithm groups the nodes in the graph by optimizing the graphs modularity.

.. py:function:: gds.beta.modularityOptimization.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.spanningTree.mutate(G: Graph, **config: Any) -> Series[Any]

    The spanning tree algorithm visits all nodes that are in the same connected component as the starting node,
    and returns a spanning tree of all nodes in the component where the total weight of the relationships is either minimized or maximized.

.. py:function:: gds.beta.spanningTree.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.spanningTree.stats(G: Graph, **config: Any) -> Series[Any]

    The spanning tree algorithm visits all nodes that are in the same connected component as the starting node,
    and returns a spanning tree of all nodes in the component
    where the total weight of the relationships is either minimized or maximized.

.. py:function:: gds.beta.spanningTree.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.spanningTree.stream(G: Graph, **config: Any) -> DataFrame

    The spanning tree algorithm visits all nodes that are in the same connected component as the starting node,
    and returns a spanning tree of all nodes in the component
    where the total weight of the relationships is either minimized or maximized.

.. py:function:: gds.beta.spanningTree.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.spanningTree.write(G: Graph, **config: Any) -> Series[Any]

    The spanning tree algorithm visits all nodes that are in the same connected component as the starting node,
    and returns a spanning tree of all nodes in the component
    where the total weight of the relationships is either minimized or maximized.

.. py:function:: gds.beta.spanningTree.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.steinerTree.mutate(G: Graph, **config: Any) -> Series[Any]

    The steiner tree algorithm accepts a source node, as well as a list of target nodes.
    It then attempts to find a spanning tree where there is a path from the source node to each target node,
    such that the total weight of the relationships is as low as possible.

.. py:function:: gds.beta.steinerTree.stats(G: Graph, **config: Any) -> Series[Any]

    The steiner tree algorithm accepts a source node, as well as a list of target nodes.
    It then attempts to find a spanning tree where there is a path from the source node to each target node,
    such that the total weight of the relationships is as low as possible.

.. py:function:: gds.beta.steinerTree.stream(G: Graph, **config: Any) -> DataFrame

    The steiner tree algorithm accepts a source node, as well as a list of target nodes.
    It then attempts to find a spanning tree where there is a path from the source node to each target node,
    such that the total weight of the relationships is as low as possible.

.. py:function:: gds.beta.steinerTree.write(G: Graph, **config: Any) -> Series[Any]

    The steiner tree algorithm accepts a source node, as well as a list of target nodes.
    It then attempts to find a spanning tree where there is a path from the source node to each target node,
    such that the total weight of the relationships is as low as possible.

.. py:function:: gds.betweenness.mutate(G: Graph, **config: Any) -> Series[Any]

    Betweenness centrality measures the relative information flow that passes through a node.

.. py:function:: gds.betweenness.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Betweenness centrality measures the relative information flow that passes through a node.

.. py:function:: gds.betweenness.stats(G: Graph, **config: Any) -> Series[Any]

    Betweenness centrality measures the relative information flow that passes through a node.

.. py:function:: gds.betweenness.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Betweenness centrality measures the relative information flow that passes through a node.

.. py:function:: gds.betweenness.stream(G: Graph, **config: Any) -> DataFrame

    Betweenness centrality measures the relative information flow that passes through a node.

.. py:function:: gds.betweenness.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Betweenness centrality measures the relative information flow that passes through a node.

.. py:function:: gds.betweenness.write(G: Graph, **config: Any) -> Series[Any]

    Betweenness centrality measures the relative information flow that passes through a node.

.. py:function:: gds.betweenness.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Betweenness centrality measures the relative information flow that passes through a node.

.. py:function:: gds.bfs.mutate(G: Graph, **config: Any) -> Series[Any]

    BFS is a traversal algorithm, which explores all of the neighbor nodes at the present depth
    prior to moving on to the nodes at the next depth level.

.. py:function:: gds.bfs.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.bfs.stats(G: Graph, **config: Any) -> Series[Any]

    BFS is a traversal algorithm, which explores all of the neighbor nodes at the present depth
    prior to moving on to the nodes at the next depth level.

.. py:function:: gds.bfs.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.bfs.stream(G: Graph, **config: Any) -> DataFrame

    BFS is a traversal algorithm, which explores all of the neighbor nodes at the present depth
    prior to moving on to the nodes at the next depth level.

.. py:function:: gds.bfs.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    BFS is a traversal algorithm, which explores all of the neighbor nodes at the present depth
    prior to moving on to the nodes at the next depth level.

.. py:function:: gds.bridges.stream(G: Graph, **config: Any) -> Series[Any]

    An algorithm to find Bridge edges in a graph.

.. py:function:: gds.bridges.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.cliqueCounting.mutate(G: Graph, **config: Any) -> DataFrame

    The Clique Counting algorithm count the number of cliques of each size for every node in the graph.

.. py:function:: gds.cliqueCounting.stats(G: Graph, **config: Any) -> DataFrame

    The Clique Counting algorithm count the number of cliques of each size for every node in the graph.

.. py:function:: gds.cliqueCounting.stream(G: Graph, **config: Any) -> DataFrame

    The Clique Counting algorithm count the number of cliques of each size for every node in the graph.

.. py:function:: gds.cliqueCounting.write(G: Graph, **config: Any) -> DataFrame

    The Clique Counting algorithm count the number of cliques of each size for every node in the graph.

.. py:function:: gds.closeness.harmonic.mutate(G: Graph, **config: Any) -> DataFrame

    Harmonic centrality is a way of detecting nodes that are able to spread information
    very efficiently through a graph.

.. py:function:: gds.closeness.harmonic.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.closeness.harmonic.stats(G: Graph, **config: Any) -> DataFrame

    Harmonic centrality is a way of detecting nodes that are able to spread information
    very efficiently through a graph.

.. py:function:: gds.closeness.harmonic.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.closeness.harmonic.stream(G: Graph, **config: Any) -> DataFrame

    Harmonic centrality is a way of detecting nodes that are able to spread information
    very efficiently through a graph.

.. py:function:: gds.closeness.harmonic.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.closeness.harmonic.write(G: Graph, **config: Any) -> Series[Any]

    Harmonic centrality is a way of detecting nodes that are able to spread information
    very efficiently through a graph.

.. py:function:: gds.closeness.harmonic.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.closeness.mutate(G: Graph, **config: Any) -> Series[Any]

    Closeness centrality is a way of detecting nodes that are able to spread information very efficiently through a graph.

.. py:function:: gds.closeness.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.closeness.stats(G: Graph, **config: Any) -> Series[Any]

    Closeness centrality is a way of detecting nodes that are able to spread information very efficiently through a graph.

.. py:function:: gds.closeness.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.closeness.stream(G: Graph, **config: Any) -> DataFrame

    Closeness centrality is a way of detecting nodes that are able to spread information very efficiently through a graph.

.. py:function:: gds.closeness.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.closeness.write(G: Graph, **config: Any) -> Series[Any]

    Closeness centrality is a way of detecting nodes that are able to spread information very efficiently through a graph.

.. py:function:: gds.closeness.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.collapsePath.mutate(G: Graph, **config: Any) -> Series[Any]

    Collapse Path algorithm is a traversal algorithm capable of creating relationships between the start
    and end nodes of a traversal

.. py:function:: gds.conductance.stream(G: Graph, **config: Any) -> DataFrame

    Evaluates a division of nodes into communities based on the proportion of relationships
    that cross community boundaries.

.. py:function:: gds.dag.longestPath.stream(G: Graph, **config: Any) -> DataFrame

    Finds the longest path that leads to a node in a directed acyclic graph (DAG).

.. py:function:: gds.dag.topologicalSort.stream(G: Graph, **config: Any) -> DataFrame

    Returns a topological ordering of the nodes in a directed acyclic graph (DAG).

.. py:function:: gds.degree.mutate(G: Graph, **config: Any) -> Series[Any]

    Degree centrality measures the number of incoming and outgoing relationships from a node.

.. py:function:: gds.degree.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Degree centrality measures the number of incoming and outgoing relationships from a node.

.. py:function:: gds.degree.stats(G: Graph, **config: Any) -> Series[Any]

    Degree centrality measures the number of incoming and outgoing relationships from a node.

.. py:function:: gds.degree.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Degree centrality measures the number of incoming and outgoing relationships from a node.

.. py:function:: gds.degree.stream(G: Graph, **config: Any) -> DataFrame

    Degree centrality measures the number of incoming and outgoing relationships from a node.

.. py:function:: gds.degree.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Degree centrality measures the number of incoming and outgoing relationships from a node.

.. py:function:: gds.degree.write(G: Graph, **config: Any) -> Series[Any]

    Degree centrality measures the number of incoming and outgoing relationships from a node.

.. py:function:: gds.degree.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Degree centrality measures the number of incoming and outgoing relationships from a node.

.. py:function:: gds.dfs.mutate(G: Graph, **config: Any) -> Series[Any]

    Depth-first search (DFS) is an algorithm for traversing or searching tree or graph data structures.
    The algorithm starts at the root node (selecting some arbitrary node as the root node in the case of a graph)
    and explores as far as possible along each branch before backtracking.

.. py:function:: gds.dfs.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.dfs.stream(G: Graph, **config: Any) -> DataFrame

    Depth-first search (DFS) is an algorithm for traversing or searching tree or graph data structures.
    The algorithm starts at the root node (selecting some arbitrary node as the root node in the case of a graph)
    and explores as far as possible along each branch before backtracking.

.. py:function:: gds.dfs.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Depth-first search (DFS) is an algorithm for traversing or searching tree or graph data structures.
    The algorithm starts at the root node (selecting some arbitrary node as the root node in the case of a graph)
    and explores as far as possible along each branch before backtracking.

.. py:function:: gds.eigenvector.mutate(G: Graph, **config: Any) -> Series[Any]

    Eigenvector Centrality is an algorithm that measures the transitive influence or connectivity of nodes.

.. py:function:: gds.eigenvector.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.eigenvector.stats(G: Graph, **config: Any) -> Series[Any]

    Eigenvector Centrality is an algorithm that measures the transitive influence or connectivity of nodes.

.. py:function:: gds.eigenvector.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.eigenvector.stream(G: Graph, **config: Any) -> DataFrame

    Eigenvector Centrality is an algorithm that measures the transitive influence or connectivity of nodes.

.. py:function:: gds.eigenvector.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.eigenvector.write(G: Graph, **config: Any) -> Series[Any]

    Eigenvector Centrality is an algorithm that measures the transitive influence or connectivity of nodes.

.. py:function:: gds.eigenvector.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.graph.sample.cnarw(graph_name: str, from_G: Graph, **config: Any) -> GraphCreateResult

    Constructs a random subgraph based on common-neighbour-aware random walks.

.. py:function:: gds.graph.sample.cnarw.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.graph.sample.rwr(graph_name: str, from_G: Graph, **config: Any) -> GraphCreateResult

    Constructs a random subgraph based on random walks with restarts.

.. py:function:: gds.hdbscan.mutate(G: Graph, **config: Any) -> Series[Any]

    Hierarchical Density-Based Spatial Clustering of Applications with Noise

.. py:function:: gds.hdbscan.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.hdbscan.stats(G: Graph, **config: Any) -> Series[Any]

    Hierarchical Density-Based Spatial Clustering of Applications with Noise

.. py:function:: gds.hdbscan.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.hdbscan.stream(G: Graph, **config: Any) -> Series[Any]

    Hierarchical Density-Based Spatial Clustering of Applications with Noise

.. py:function:: gds.hdbscan.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.hdbscan.write(G: Graph, **config: Any) -> Series[Any]

    Hierarchical Density-Based Spatial Clustering of Applications with Noise

.. py:function:: gds.hdbscan.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.hits.mutate(G: Graph, **config: Any) -> Series[Any]

    Hyperlink-Induced Topic Search (HITS) is a link analysis algorithm that rates nodes.

.. py:function:: gds.hits.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.hits.stats(G: Graph, **config: Any) -> Series[Any]

    Hyperlink-Induced Topic Search (HITS) is a link analysis algorithm that rates nodes.

.. py:function:: gds.hits.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.hits.stream(G: Graph, **config: Any) -> DataFrame

    Hyperlink-Induced Topic Search (HITS) is a link analysis algorithm that rates nodes.

.. py:function:: gds.hits.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.hits.write(G: Graph, **config: Any) -> Series[Any]

    Hyperlink-Induced Topic Search (HITS) is a link analysis algorithm that rates nodes.

.. py:function:: gds.hits.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.influenceMaximization.celf.mutate(G: Graph, **config: Any) -> Series[Any]

    The Cost Effective Lazy Forward (CELF) algorithm aims to find k nodes
    that maximize the expected spread of influence in the network.

.. py:function:: gds.influenceMaximization.celf.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.influenceMaximization.celf.stats(G: Graph, **config: Any) -> Series[Any]

    Executes the algorithm and returns result statistics without writing the result to Neo4j.

.. py:function:: gds.influenceMaximization.celf.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.influenceMaximization.celf.stream(G: Graph, **config: Any) -> DataFrame

    The Cost Effective Lazy Forward (CELF) algorithm aims to find k nodes
    that maximize the expected spread of influence in the network.

.. py:function:: gds.influenceMaximization.celf.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    The Cost Effective Lazy Forward (CELF) algorithm aims to find k nodes
    that maximize the expected spread of influence in the network.

.. py:function:: gds.influenceMaximization.celf.write(G: Graph, **config: Any) -> Series[Any]

    The Cost Effective Lazy Forward (CELF) algorithm aims to find k nodes
    that maximize the expected spread of influence in the network.

.. py:function:: gds.influenceMaximization.celf.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.k1coloring.mutate(G: Graph, **config: Any) -> Series[Any]

    The K-1 Coloring algorithm assigns a color to every node in the graph.

.. py:function:: gds.k1coloring.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.k1coloring.stats(G: Graph, **config: Any) -> Series[Any]

    The K-1 Coloring algorithm assigns a color to every node in the graph.

.. py:function:: gds.k1coloring.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.k1coloring.stream(G: Graph, **config: Any) -> DataFrame

    The K-1 Coloring algorithm assigns a color to every node in the graph.

.. py:function:: gds.k1coloring.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.k1coloring.write(G: Graph, **config: Any) -> Series[Any]

    The K-1 Coloring algorithm assigns a color to every node in the graph.

.. py:function:: gds.k1coloring.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.kSpanningTree.write(G: Graph, **config: Any) -> Series[Any]

    The K-spanning tree algorithm starts from a root node and returns a spanning tree with exactly k nodes

.. py:function:: gds.kcore.mutate(G: Graph, **config: Any) -> Series[Any]

    Computes the k-core values in a network

.. py:function:: gds.kcore.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.kcore.stats(G: Graph, **config: Any) -> Series[Any]

    Computes the k-core values in a network

.. py:function:: gds.kcore.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.kcore.stream(G: Graph, **config: Any) -> Series[Any]

    Computes the k-core values in a network

.. py:function:: gds.kcore.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.kcore.write(G: Graph, **config: Any) -> Series[Any]

    Computes the k-core values in a network

.. py:function:: gds.kcore.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.kmeans.mutate(G: Graph, **config: Any) -> Series[Any]

    The Kmeans  algorithm clusters nodes into different communities based on Euclidean distance

.. py:function:: gds.kmeans.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.kmeans.stats(G: Graph, **config: Any) -> Series[Any]

    The Kmeans  algorithm clusters nodes into different communities based on Euclidean distance

.. py:function:: gds.kmeans.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.kmeans.stream(G: Graph, **config: Any) -> DataFrame

    The Kmeans  algorithm clusters nodes into different communities based on Euclidean distance

.. py:function:: gds.kmeans.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.kmeans.write(G: Graph, **config: Any) -> Series[Any]

    The Kmeans  algorithm clusters nodes into different communities based on Euclidean distance

.. py:function:: gds.kmeans.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.knn.filtered.mutate(G: Graph, **config: Any) -> Series[Any]

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties.
    Filtered KNN extends this functionality, allowing filtering on source nodes and target nodes, respectively.
    
    .. py:function:: gds.knn.filtered.mutate.estimate(G: Graph, **config: Any) -> Series[Any]
    
    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.knn.filtered.stats(G: Graph, **config: Any) -> Series[Any]

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties.
    Filtered KNN extends this functionality, allowing filtering on source nodes and target nodes, respectively.

.. py:function:: gds.knn.filtered.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.knn.filtered.stream(G: Graph, **config: Any) -> DataFrame

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties.
    Filtered KNN extends this functionality, allowing filtering on source nodes and target nodes, respectively.

.. py:function:: gds.knn.filtered.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.knn.filtered.write(G: Graph, **config: Any) -> Series[Any]

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties.
    Filtered KNN extends this functionality, allowing filtering on source nodes and target nodes, respectively.

.. py:function:: gds.knn.filtered.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.knn.mutate(G: Graph, **config: Any) -> Series[Any]

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties

.. py:function:: gds.knn.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.knn.stats(G: Graph, **config: Any) -> Series[Any]

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties

.. py:function:: gds.knn.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.knn.stream(G: Graph, **config: Any) -> DataFrame

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties

.. py:function:: gds.knn.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.knn.write(G: Graph, **config: Any) -> Series[Any]

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties

.. py:function:: gds.knn.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.labelPropagation.mutate(G: Graph, **config: Any) -> Series[Any]

    The Label Propagation algorithm is a fast algorithm for finding communities in a graph.

.. py:function:: gds.labelPropagation.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.labelPropagation.stats(G: Graph, **config: Any) -> Series[Any]

    The Label Propagation algorithm is a fast algorithm for finding communities in a graph.

.. py:function:: gds.labelPropagation.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.labelPropagation.stream(G: Graph, **config: Any) -> DataFrame

    The Label Propagation algorithm is a fast algorithm for finding communities in a graph.

.. py:function:: gds.labelPropagation.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.labelPropagation.write(G: Graph, **config: Any) -> Series[Any]

    The Label Propagation algorithm is a fast algorithm for finding communities in a graph.

.. py:function:: gds.labelPropagation.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.leiden.mutate(G: Graph, **config: Any) -> Series[Any]

    Leiden is a community detection algorithm, which guarantees that communities are well connected

.. py:function:: gds.leiden.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.leiden.stats(G: Graph, **config: Any) -> Series[Any]

    Executes the algorithm and returns result statistics without writing the result to Neo4j.

.. py:function:: gds.leiden.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.leiden.stream(G: Graph, **config: Any) -> DataFrame

    Leiden is a community detection algorithm, which guarantees that communities are well connected

.. py:function:: gds.leiden.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.leiden.write(G: Graph, **config: Any) -> Series[Any]

    Leiden is a community detection algorithm, which guarantees that communities are well connected

.. py:function:: gds.leiden.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.localClusteringCoefficient.mutate(G: Graph, **config: Any) -> Series[Any]

    The local clustering coefficient is a metric quantifying how connected the neighborhood of a node is.

.. py:function:: gds.localClusteringCoefficient.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.localClusteringCoefficient.stats(G: Graph, **config: Any) -> Series[Any]

    Executes the algorithm and returns result statistics without writing the result to Neo4j.

.. py:function:: gds.localClusteringCoefficient.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.localClusteringCoefficient.stream(G: Graph, **config: Any) -> DataFrame

    The local clustering coefficient is a metric quantifying how connected the neighborhood of a node is.

.. py:function:: gds.localClusteringCoefficient.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.localClusteringCoefficient.write(G: Graph, **config: Any) -> Series[Any]

    The local clustering coefficient is a metric quantifying how connected the neighborhood of a node is.

.. py:function:: gds.localClusteringCoefficient.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.louvain.mutate(G: Graph, **config: Any) -> Series[Any]

    The Louvain method for community detection is an algorithm for detecting communities in networks.

.. py:function:: gds.louvain.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.louvain.stats(G: Graph, **config: Any) -> Series[Any]

    Executes the algorithm and returns result statistics without writing the result to Neo4j.

.. py:function:: gds.louvain.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.louvain.stream(G: Graph, **config: Any) -> DataFrame

    The Louvain method for community detection is an algorithm for detecting communities in networks.

.. py:function:: gds.louvain.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.louvain.write(G: Graph, **config: Any) -> Series[Any]

    The Louvain method for community detection is an algorithm for detecting communities in networks.

.. py:function:: gds.louvain.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.maxkcut.mutate(G: Graph, **config: Any) -> Series[Any]

    Approximate Maximum k-cut maps each node into one of k disjoint communities
    trying to maximize the sum of weights of relationships between these communities.

.. py:function:: gds.maxkcut.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Approximate Maximum k-cut maps each node into one of k disjoint communities
    trying to maximize the sum of weights of relationships between these communities.

.. py:function:: gds.maxkcut.stream(G: Graph, **config: Any) -> DataFrame

    Approximate Maximum k-cut maps each node into one of k disjoint communities
    trying to maximize the sum of weights of relationships between these communities.

.. py:function:: gds.maxkcut.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Approximate Maximum k-cut maps each node into one of k disjoint communities
    trying to maximize the sum of weights of relationships between these communities.

.. py:function:: gds.modularity.stats(G: Graph, **config: Any) -> Series[Any]

.. py:function:: gds.modularity.stats.estimate(G: Graph, **config: Any) -> Series[Any]

.. py:function:: gds.modularity.stream(G: Graph, **config: Any) -> DataFrame

.. py:function:: gds.modularity.stream.estimate(G: Graph, **config: Any) -> Series[Any]

.. py:function:: gds.modularityOptimization.mutate(G: Graph, **config: Any) -> Series[Any]

    The Modularity Optimization algorithm groups the nodes in the graph by optimizing the graphs modularity.

.. py:function:: gds.modularityOptimization.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.modularityOptimization.stats(G: Graph, **config: Any) -> Series[Any]

    The Modularity Optimization algorithm groups the nodes in the graph by optimizing the graphs modularity.

.. py:function:: gds.modularityOptimization.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.modularityOptimization.stream(G: Graph, **config: Any) -> DataFrame

    The Modularity Optimization algorithm groups the nodes in the graph by optimizing the graphs modularity.

.. py:function:: gds.modularityOptimization.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.modularityOptimization.write(G: Graph, **config: Any) -> Series[Any]

    The Modularity Optimization algorithm groups the nodes in the graph by optimizing the graphs modularity.

.. py:function:: gds.modularityOptimization.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.nodeSimilarity.filtered.mutate(G: Graph, **config: Any) -> Series[Any]

    The Filtered Node Similarity algorithm compares a set of nodes based on the nodes they are connected to.
    Two nodes are considered similar if they share many of the same neighbors.
    The algorithm computes pair-wise similarities based on Jaccard or Overlap metrics.
    The filtered variant supports limiting which nodes to compare via source and target node filters.

.. py:function:: gds.nodeSimilarity.filtered.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.nodeSimilarity.filtered.stats(G: Graph, **config: Any) -> Series[Any]

    The Filtered Node Similarity algorithm compares a set of nodes based on the nodes they are connected to.
    Two nodes are considered similar if they share many of the same neighbors.
    The algorithm computes pair-wise similarities based on Jaccard or Overlap metrics.
    The filtered variant supports limiting which nodes to compare via source and target node filters.

.. py:function:: gds.nodeSimilarity.filtered.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.nodeSimilarity.filtered.stream(G: Graph, **config: Any) -> DataFrame

    The Filtered Node Similarity algorithm compares a set of nodes based on the nodes they are connected to.
    Two nodes are considered similar if they share many of the same neighbors.
    The algorithm computes pair-wise similarities based on Jaccard or Overlap metrics.
    The filtered variant supports limiting which nodes to compare via source and target node filters.

.. py:function:: gds.nodeSimilarity.filtered.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.nodeSimilarity.filtered.write(G: Graph, **config: Any) -> Series[Any]

    The Filtered Node Similarity algorithm compares a set of nodes based on the nodes they are connected to.
    Two nodes are considered similar if they share many of the same neighbors.
    The algorithm computes pair-wise similarities based on Jaccard or Overlap metrics.
    The filtered variant supports limiting which nodes to compare via source and target node filters.

.. py:function:: gds.nodeSimilarity.filtered.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.nodeSimilarity.mutate(G: Graph, **config: Any) -> Series[Any]

    The Node Similarity algorithm compares a set of nodes based on the nodes they are connected to.
    Two nodes are considered similar if they share many of the same neighbors.
    Node Similarity computes pair-wise similarities based on the Jaccard metric.

.. py:function:: gds.nodeSimilarity.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.nodeSimilarity.stats(G: Graph, **config: Any) -> Series[Any]

    The Node Similarity algorithm compares a set of nodes based on the nodes they are connected to.
    Two nodes are considered similar if they share many of the same neighbors.
    Node Similarity computes pair-wise similarities based on the Jaccard metric.

.. py:function:: gds.nodeSimilarity.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.nodeSimilarity.stream(G: Graph, **config: Any) -> DataFrame

    The Node Similarity algorithm compares a set of nodes based on the nodes they are connected to.
    Two nodes are considered similar if they share many of the same neighbors.
    Node Similarity computes pair-wise similarities based on the Jaccard metric.

.. py:function:: gds.nodeSimilarity.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.nodeSimilarity.write(G: Graph, **config: Any) -> Series[Any]

    The Node Similarity algorithm compares a set of nodes based on the nodes they are connected to.
    Two nodes are considered similar if they share many of the same neighbors.
    Node Similarity computes pair-wise similarities based on the Jaccard metric.

.. py:function:: gds.nodeSimilarity.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.pageRank.mutate(G: Graph, **config: Any) -> Series[Any]

    Page Rank is an algorithm that measures the transitive influence or connectivity of nodes.

.. py:function:: gds.pageRank.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.pageRank.stats(G: Graph, **config: Any) -> Series[Any]

    Executes the algorithm and returns result statistics without writing the result to Neo4j.

.. py:function:: gds.pageRank.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.pageRank.stream(G: Graph, **config: Any) -> DataFrame

    Page Rank is an algorithm that measures the transitive influence or connectivity of nodes.

.. py:function:: gds.pageRank.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.pageRank.write(G: Graph, **config: Any) -> Series[Any]

    Page Rank is an algorithm that measures the transitive influence or connectivity of nodes.

.. py:function:: gds.pageRank.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.prizeSteinerTree.mutate(G: Graph, **config: Any) -> DataFrame

    An approximation algorithm for the prize collector steiner tree problem

.. py:function:: gds.prizeSteinerTree.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.prizeSteinerTree.stats(G: Graph, **config: Any) -> DataFrame

    An approximation algorithm for the prize collector steiner tree problem

.. py:function:: gds.prizeSteinerTree.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.prizeSteinerTree.stream(G: Graph, **config: Any) -> DataFrame

    An approximation algorithm for the prize collector steiner tree problem

.. py:function:: gds.prizeSteinerTree.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.prizeSteinerTree.write(G: Graph, **config: Any) -> DataFrame

    An approximation algorithm for the prize collector steiner tree problem

.. py:function:: gds.prizeSteinerTree.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.randomWalk.mutate(G: Graph, **config: Any) -> Series[Any]

    Random Walk is an algorithm that provides random paths in a graph. It’s similar to how a drunk person traverses a city. The mutate procedure produces the count a node occurs on a random walk.

.. py:function:: gds.randomWalk.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.randomWalk.stats(G: Graph, **config: Any) -> Series[Any]

    Random Walk is an algorithm that provides random paths in a graph. It’s similar to how a drunk person traverses a city.

.. py:function:: gds.randomWalk.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.randomWalk.stream(G: Graph, **config: Any) -> DataFrame

    Random Walk is an algorithm that provides random paths in a graph. It’s similar to how a drunk person traverses a city.

.. py:function:: gds.randomWalk.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.scaleProperties.mutate(G: Graph, **config: Any) -> Series[Any]

    Scale node properties

.. py:function:: gds.scaleProperties.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.scaleProperties.stats(G: Graph, **config: Any) -> Series[Any]

    Scale node properties

.. py:function:: gds.scaleProperties.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.scaleProperties.stream(G: Graph, **config: Any) -> DataFrame

    Scale node properties

.. py:function:: gds.scaleProperties.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.scaleProperties.write(G: Graph, **config: Any) -> Series[Any]

    Scale node properties

.. py:function:: gds.scaleProperties.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.scc.mutate(G: Graph, **config: Any) -> Series[Any]

    The SCC algorithm finds sets of connected nodes in an directed graph, where all nodes in the same set form a connected component.

.. py:function:: gds.scc.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for SCC.

.. py:function:: gds.scc.stats(G: Graph, **config: Any) -> Series[Any]

    The SCC algorithm finds sets of connected nodes in an directed graph, where all nodes in the same set form a connected component.

.. py:function:: gds.scc.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for SCC.

.. py:function:: gds.scc.stream(G: Graph, **config: Any) -> DataFrame

    The SCC algorithm finds sets of connected nodes in an directed graph, where all nodes in the same set form a connected component.

.. py:function:: gds.scc.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for SCC.

.. py:function:: gds.scc.write(G: Graph, **config: Any) -> Series[Any]

    The SCC algorithm finds sets of connected nodes in an directed graph, where all nodes in the same set form a connected component.

.. py:function:: gds.scc.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for SCC.

.. py:function:: gds.shortestPath.astar.mutate(G: Graph, **config: Any) -> Series[Any]

    The A* shortest path algorithm computes the shortest path between a pair of nodes. It uses the relationship weight
    property to compare path lengths. In addition,
    this implementation uses the haversine distance as a heuristic to converge faster.

.. py:function:: gds.shortestPath.astar.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.shortestPath.astar.stream(G: Graph, **config: Any) -> DataFrame

    The A* shortest path algorithm computes the shortest path between a pair of nodes. It uses the relationship weight
    property to compare path lengths. In addition,
    this implementation uses the haversine distance as a heuristic to converge faster.

.. py:function:: gds.shortestPath.astar.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.shortestPath.astar.write(G: Graph, **config: Any) -> Series[Any]

    The A* shortest path algorithm computes the shortest path between a pair of nodes. It uses the relationship weight
    property to compare path lengths. In addition,
    this implementation uses the haversine distance as a heuristic to converge faster.

.. py:function:: gds.shortestPath.astar.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.shortestPath.dijkstra.mutate(G: Graph, **config: Any) -> Series[Any]

    The Dijkstra shortest path algorithm computes the shortest (weighted) path between a pair of nodes.

.. py:function:: gds.shortestPath.dijkstra.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.shortestPath.dijkstra.stream(G: Graph, **config: Any) -> DataFrame

    The Dijkstra shortest path algorithm computes the shortest (weighted) path between a pair of nodes.

.. py:function:: gds.shortestPath.dijkstra.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.shortestPath.dijkstra.write(G: Graph, **config: Any) -> Series[Any]

    The Dijkstra shortest path algorithm computes the shortest (weighted) path between a pair of nodes.

.. py:function:: gds.shortestPath.dijkstra.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.shortestPath.yens.mutate(G: Graph, **config: Any) -> Series[Any]

    The Yen's shortest path algorithm computes the k shortest (weighted) paths between a pair of nodes.

.. py:function:: gds.shortestPath.yens.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.shortestPath.yens.stream(G: Graph, **config: Any) -> DataFrame

    The Yen's shortest path algorithm computes the k shortest (weighted) paths between a pair of nodes.

.. py:function:: gds.shortestPath.yens.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.shortestPath.yens.write(G: Graph, **config: Any) -> Series[Any]

    The Yen's shortest path algorithm computes the k shortest (weighted) paths between a pair of nodes.

.. py:function:: gds.shortestPath.yens.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.sllpa.mutate(G: Graph, **config: Any) -> Series[Any]

    The Speaker Listener Label Propagation algorithm is a fast algorithm for finding overlapping communities in a graph.

.. py:function:: gds.sllpa.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.sllpa.stats(G: Graph, **config: Any) -> Series[Any]

    The Speaker Listener Label Propagation algorithm is a fast algorithm for finding overlapping communities in a graph.

.. py:function:: gds.sllpa.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.sllpa.stream(G: Graph, **config: Any) -> DataFrame

    The Speaker Listener Label Propagation algorithm is a fast algorithm for finding overlapping communities in a graph.

.. py:function:: gds.sllpa.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.sllpa.write(G: Graph, **config: Any) -> Series[Any]

    The Speaker Listener Label Propagation algorithm is a fast algorithm for finding overlapping communities in a graph.

.. py:function:: gds.sllpa.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.spanningTree.mutate(G: Graph, **config: Any) -> Series[Any]

    The spanning tree algorithm visits all nodes that are in the same connected component as the starting node,
    and returns a spanning tree of all nodes in the component where the total weight of the relationships is either minimized or maximized.

.. py:function:: gds.spanningTree.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.spanningTree.stats(G: Graph, **config: Any) -> Series[Any]

    The spanning tree algorithm visits all nodes that are in the same connected component as the starting node,
    and returns a spanning tree of all nodes in the component
    where the total weight of the relationships is either minimized or maximized.

.. py:function:: gds.spanningTree.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.spanningTree.stream(G: Graph, **config: Any) -> DataFrame

    The spanning tree algorithm visits all nodes that are in the same connected component as the starting node,
    and returns a spanning tree of all nodes in the component
    where the total weight of the relationships is either minimized or maximized.

.. py:function:: gds.spanningTree.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.spanningTree.write(G: Graph, **config: Any) -> Series[Any]

    The spanning tree algorithm visits all nodes that are in the same connected component as the starting node,
    and returns a spanning tree of all nodes in the component
    where the total weight of the relationships is either minimized or maximized.

.. py:function:: gds.spanningTree.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.steinerTree.mutate(G: Graph, **config: Any) -> Series[Any]

    The steiner tree algorithm accepts a source node, as well as a list of target nodes.
    It then attempts to find a spanning tree where there is a path from the source node to each target node,
    such that the total weight of the relationships is as low as possible.

.. py:function:: gds.steinerTree.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.steinerTree.stats(G: Graph, **config: Any) -> Series[Any]

    The steiner tree algorithm accepts a source node, as well as a list of target nodes.
    It then attempts to find a spanning tree where there is a path from the source node to each target node,
    such that the total weight of the relationships is as low as possible.

.. py:function:: gds.steinerTree.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.steinerTree.stream(G: Graph, **config: Any) -> DataFrame

    The steiner tree algorithm accepts a source node, as well as a list of target nodes.
    It then attempts to find a spanning tree where there is a path from the source node to each target node,
    such that the total weight of the relationships is as low as possible.

.. py:function:: gds.steinerTree.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.steinerTree.write(G: Graph, **config: Any) -> Series[Any]

    The steiner tree algorithm accepts a source node, as well as a list of target nodes.
    It then attempts to find a spanning tree where there is a path from the source node to each target node,
    such that the total weight of the relationships is as low as possible.

.. py:function:: gds.steinerTree.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.triangleCount.mutate(G: Graph, **config: Any) -> Series[Any]

    Triangle counting is a community detection graph algorithm that is used to
    determine the number of triangles passing through each node in the graph.

.. py:function:: gds.triangleCount.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.triangleCount.stats(G: Graph, **config: Any) -> Series[Any]

    Triangle counting is a community detection graph algorithm that is used to
    determine the number of triangles passing through each node in the graph.

.. py:function:: gds.triangleCount.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.triangleCount.stream(G: Graph, **config: Any) -> DataFrame

    Triangle counting is a community detection graph algorithm that is used to
    determine the number of triangles passing through each node in the graph.

.. py:function:: gds.triangleCount.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.triangleCount.write(G: Graph, **config: Any) -> Series[Any]

    Triangle counting is a community detection graph algorithm that is used to
    determine the number of triangles passing through each node in the graph.

.. py:function:: gds.triangleCount.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Triangle counting is a community detection graph algorithm that is used to
    determine the number of triangles passing through each node in the graph.

.. py:function:: gds.triangles(G: Graph, **config: Any) -> DataFrame

    Triangles streams the nodeIds of each triangle in the graph.

.. py:function:: gds.wcc.mutate(G: Graph, **config: Any) -> Series[Any]

    The WCC algorithm finds sets of connected nodes in an undirected graph,
    where all nodes in the same set form a connected component.

.. py:function:: gds.wcc.mutate.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.wcc.stats(G: Graph, **config: Any) -> Series[Any]

    Executes the algorithm and returns result statistics without writing the result to Neo4j.

.. py:function:: gds.wcc.stats.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.wcc.stream(G: Graph, **config: Any) -> DataFrame

    The WCC algorithm finds sets of connected nodes in an undirected graph,
    where all nodes in the same set form a connected component.

.. py:function:: gds.wcc.stream.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.wcc.write(G: Graph, **config: Any) -> Series[Any]

    The WCC algorithm finds sets of connected nodes in an undirected graph,
    where all nodes in the same set form a connected component.

.. py:function:: gds.wcc.write.estimate(G: Graph, **config: Any) -> Series[Any]

    Returns an estimation of the memory consumption for that procedure.

