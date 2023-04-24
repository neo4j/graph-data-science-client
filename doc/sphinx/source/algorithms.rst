Algorithms procedures
----------------------
All the graph procedures under the `gds` namespace.

.. py:function:: gds.allShortestPaths.delta.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The Delta Stepping shortest path algorithm computes the shortest (weighted) path
    between one node and any other node in the graph. The computation is run multi-threaded.

.. py:function:: gds.allShortestPaths.delta.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for allShortestPaths.dealta.mutate.

.. py:function:: gds.allShortestPaths.delta.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    The Delta Stepping shortest path algorithm computes the shortest (weighted) path
    between one node and any other node in the graph. The computation is run multi-threaded

.. py:function:: gds.allShortestPaths.delta.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for allShortestPaths.delta.stats.

.. py:function:: gds.allShortestPaths.delta.stream(self, G: Graph, **config: Any) -> DataFrame

    The Delta Stepping shortest path algorithm computes the shortest (weighted) path
    between one node and any other node in the graph. The computation is run multi-threaded.

.. py:function:: gds.allShortestPaths.delta.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for allShortestPaths.delta.strema.

.. py:function:: gds.allShortestPaths.delta.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The Delta Stepping shortest path algorithm computes the shortest (weighted) path
    between one node and any other node in the graph. The computation is run multi-threaded.

.. py:function:: gds.allShortestPaths.delta.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.allShortestPaths.dijkstra.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The Dijkstra shortest path algorithm computes the shortest (weighted) path
    between one node and any other node in the graph.

.. py:function:: gds.allShortestPaths.dijkstra.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.allShortestPaths.dijkstra.stream(self, G: Graph, **config: Any) -> DataFrame

    The Dijkstra shortest path algorithm computes the shortest (weighted) path
    between one node and any other node in the graph.

.. py:function:: gds.allShortestPaths.dijkstra.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.allShortestPaths.dijkstra.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The Dijkstra shortest path algorithm computes the shortest (weighted) path
    between one node and any other node in the graph.

.. py:function:: gds.allShortestPaths.dijkstra.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.allShortestPaths.stream(self, G: Graph, **config: Any) -> DataFrame

    The All Pairs Shortest Path (APSP) calculates the shortest (weighted) path
    between all pairs of nodes.

.. py:function:: gds.alpha.closeness.harmonic.stream(self, G: Graph, **config: Any) -> DataFrame

    Harmonic centrality is a way of detecting nodes that are able to spread information
    very efficiently through a graph.

.. py:function:: gds.alpha.closeness.harmonic.write(self, G: Graph, **config: Any) -> "Series[Any]"

    Harmonic centrality is a way of detecting nodes that are able to spread information
    very efficiently through a graph.

.. py:function:: gds.alpha.conductance.stream(self, G: Graph, **config: Any) -> DataFrame

    Evaluates a division of nodes into communities based on the proportion of relationships
    that cross community boundaries.


.. py:function:: gds.alpha.graph.sample.rwr(self, graph_name: str, from_G: Graph, **config: Any)
    -> Tuple[Graph, "Series[Any]"]

    Constructs a random subgraph based on random walks with restarts.

.. py:function:: gds.alpha.hits.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    Hyperlink-Induced Topic Search (HITS) is a link analysis algorithm that rates nodes.

.. py:function:: gds.alpha.hits.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.hits.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    Hyperlink-Induced Topic Search (HITS) is a link analysis algorithm that rates nodes.

.. py:function:: gds.alpha.hits.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.hits.stream(self, G: Graph, **config: Any) -> DataFrame

    Hyperlink-Induced Topic Search (HITS) is a link analysis algorithm that rates nodes.

.. py:function:: gds.alpha.hits.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.hits.write(self, G: Graph, **config: Any) -> "Series[Any]"

    Hyperlink-Induced Topic Search (HITS) is a link analysis algorithm that rates nodes.

.. py:function:: gds.alpha.hits.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.kSpanningTree.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The K-spanning tree algorithm starts from a root node and returns a spanning tree with exactly k nodes

.. py:function:: gds.alpha.knn.filtered.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties.
    Filtered KNN extends this functionality, allowing filtering on source nodes and target nodes, respectively.

.. py:function:: gds.alpha.knn.filtered.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties.
    Filtered KNN extends this functionality, allowing filtering on source nodes and target nodes, respectively.

.. py:function:: gds.alpha.knn.filtered.stream(self, G: Graph, **config: Any) -> DataFrame

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties.
    Filtered KNN extends this functionality, allowing filtering on source nodes and target nodes, respectively.

.. py:function:: gds.alpha.knn.filtered.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties.
    Filtered KNN extends this functionality, allowing filtering on source nodes and target nodes, respectively.

.. py:function:: gds.alpha.maxkcut.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    Approximate Maximum k-cut maps each node into one of k disjoint communities
    trying to maximize the sum of weights of relationships between these communities.

.. py:function:: gds.alpha.maxkcut.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Approximate Maximum k-cut maps each node into one of k disjoint communities
    trying to maximize the sum of weights of relationships between these communities.

.. py:function:: gds.alpha.maxkcut.stream(self, G: Graph, **config: Any) -> DataFrame

    Approximate Maximum k-cut maps each node into one of k disjoint communities
    trying to maximize the sum of weights of relationships between these communities.

.. py:function:: gds.alpha.maxkcut.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Approximate Maximum k-cut maps each node into one of k disjoint communities
    trying to maximize the sum of weights of relationships between these communities.

.. py:function:: gds.alpha.modularity.stats(self, G: Graph, **config: Any) -> "Series[Any]"

.. py:function:: gds.alpha.modularity.stream(self, G: Graph, **config: Any) -> DataFrame

.. py:function:: gds.alpha.nodeSimilarity.filtered.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The Filtered Node Similarity algorithm compares a set of nodes based on the nodes they are connected to.
    Two nodes are considered similar if they share many of the same neighbors.
    The algorithm computes pair-wise similarities based on Jaccard or Overlap metrics.
    The filtered variant supports limiting which nodes to compare via source and target node filters.

.. py:function:: gds.alpha.nodeSimilarity.filtered.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.nodeSimilarity.filtered.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    The Filtered Node Similarity algorithm compares a set of nodes based on the nodes they are connected to.
    Two nodes are considered similar if they share many of the same neighbors.
    The algorithm computes pair-wise similarities based on Jaccard or Overlap metrics.
    The filtered variant supports limiting which nodes to compare via source and target node filters.

.. py:function:: gds.alpha.nodeSimilarity.filtered.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.nodeSimilarity.filtered.stream(self, G: Graph, **config: Any) -> DataFrame

    The Filtered Node Similarity algorithm compares a set of nodes based on the nodes they are connected to.
    Two nodes are considered similar if they share many of the same neighbors.
    The algorithm computes pair-wise similarities based on Jaccard or Overlap metrics.
    The filtered variant supports limiting which nodes to compare via source and target node filters.

.. py:function:: gds.alpha.nodeSimilarity.filtered.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.nodeSimilarity.filtered.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The Filtered Node Similarity algorithm compares a set of nodes based on the nodes they are connected to.
    Two nodes are considered similar if they share many of the same neighbors.
    The algorithm computes pair-wise similarities based on Jaccard or Overlap metrics.
    The filtered variant supports limiting which nodes to compare via source and target node filters.

.. py:function:: gds.alpha.nodeSimilarity.filtered.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.scc.stream(self, G: Graph, **config: Any) -> DataFrame

    The SCC algorithm finds sets of connected nodes in an directed graph,
    where all nodes in the same set form a connected component.

.. py:function:: gds.alpha.scc.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The SCC algorithm finds sets of connected nodes in an directed graph,
    where all nodes in the same set form a connected component.

.. py:function:: gds.alpha.sllpa.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The Speaker Listener Label Propagation algorithm is a fast algorithm for finding overlapping communities in a graph.

.. py:function:: gds.alpha.sllpa.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.sllpa.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    The Speaker Listener Label Propagation algorithm is a fast algorithm for finding overlapping communities in a graph.

.. py:function:: gds.alpha.sllpa.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.sllpa.stream(self, G: Graph, **config: Any) -> DataFrame

    The Speaker Listener Label Propagation algorithm is a fast algorithm for finding overlapping communities in a graph.

.. py:function:: gds.alpha.sllpa.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.sllpa.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The Speaker Listener Label Propagation algorithm is a fast algorithm for finding overlapping communities in a graph.

.. py:function:: gds.alpha.sllpa.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.triangles(self, G: Graph, **config: Any) -> DataFrame

    Triangles streams the nodeIds of each triangle in the graph.

.. py:function:: gds.articleRank.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    Article Rank is a variant of the Page Rank algorithm, which measures the transitive influence or connectivity of nodes.

.. py:function:: gds.articleRank.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.articleRank.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    Executes the algorithm and returns result statistics without writing the result to Neo4j.

.. py:function:: gds.articleRank.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.articleRank.stream(self, G: Graph, **config: Any) -> DataFrame

    Article Rank is a variant of the Page Rank algorithm, which measures the transitive influence or connectivity of nodes.

.. py:function:: gds.articleRank.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.articleRank.write(self, G: Graph, **config: Any) -> "Series[Any]"

    Article Rank is a variant of the Page Rank algorithm, which measures the transitive influence or connectivity of nodes.

.. py:function:: gds.articleRank.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.bellmanFord.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The Bellman-Ford shortest path algorithm computes the shortest (weighted) path between one node
    and any other node in the graph without negative cycles.

.. py:function:: gds.bellmanFord.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.bellmanFord.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    The Bellman-Ford shortest path algorithm computes the shortest (weighted) path between one node
    and any other node in the graph without negative cycles.

.. py:function:: gds.bellmanFord.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.bellmanFord.stream(self, G: Graph, **config: Any) -> DataFrame

    The Bellman-Ford shortest path algorithm computes the shortest (weighted) path between one node 
    and any other node in the graph without negative cycles.

.. py:function:: gds.bellmanFord.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.bellmanFord.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The Bellman-Ford shortest path algorithm computes the shortest (weighted) path between one node 
    and any other node in the graph without negative cycles.

.. py:function:: gds.bellmanFord.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.closeness.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    Closeness centrality is a way of detecting nodes that are able to spread information very efficiently through a graph.

.. py:function:: gds.beta.closeness.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    Closeness centrality is a way of detecting nodes that are able to spread information very efficiently through a graph.

.. py:function:: gds.beta.closeness.stream(self, G: Graph, **config: Any) -> DataFrame

    Closeness centrality is a way of detecting nodes that are able to spread information very efficiently through a graph.

.. py:function:: gds.beta.closeness.write(self, G: Graph, **config: Any) -> "Series[Any]"

    Closeness centrality is a way of detecting nodes that are able to spread information very efficiently through a graph.

.. py:function:: gds.beta.collapsePath.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    Collapse Path algorithm is a traversal algorithm capable of creating relationships between the start 
    and end nodes of a traversal

.. py:function:: gds.beta.influenceMaximization.celf.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The Cost Effective Lazy Forward (CELF) algorithm aims to find k nodes 
    that maximize the expected spread of influence in the network.

.. py:function:: gds.beta.influenceMaximization.celf.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.influenceMaximization.celf.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    Executes the algorithm and returns result statistics without writing the result to Neo4j.

.. py:function:: gds.beta.influenceMaximization.celf.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.influenceMaximization.celf.stream(self, G: Graph, **config: Any) -> DataFrame

    The Cost Effective Lazy Forward (CELF) algorithm aims to find k nodes
    that maximize the expected spread of influence in the network.

.. py:function:: gds.beta.influenceMaximization.celf.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    The Cost Effective Lazy Forward (CELF) algorithm aims to find k nodes
    that maximize the expected spread of influence in the network.

.. py:function:: gds.beta.influenceMaximization.celf.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The Cost Effective Lazy Forward (CELF) algorithm aims to find k nodes
    that maximize the expected spread of influence in the network.

.. py:function:: gds.beta.influenceMaximization.celf.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.k1coloring.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The K-1 Coloring algorithm assigns a color to every node in the graph.

.. py:function:: gds.beta.k1coloring.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.k1coloring.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    The K-1 Coloring algorithm assigns a color to every node in the graph.

.. py:function:: gds.beta.k1coloring.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.k1coloring.stream(self, G: Graph, **config: Any) -> DataFrame

    The K-1 Coloring algorithm assigns a color to every node in the graph.

.. py:function:: gds.beta.k1coloring.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.k1coloring.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The K-1 Coloring algorithm assigns a color to every node in the graph.

.. py:function:: gds.beta.k1coloring.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.kmeans.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The Kmeans  algorithm clusters nodes into different communities based on Euclidean distance

.. py:function:: gds.beta.kmeans.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.kmeans.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    The Kmeans  algorithm clusters nodes into different communities based on Euclidean distance

.. py:function:: gds.beta.kmeans.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.kmeans.stream(self, G: Graph, **config: Any) -> DataFrame

    The Kmeans  algorithm clusters nodes into different communities based on Euclidean distance

.. py:function:: gds.beta.kmeans.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.kmeans.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The Kmeans  algorithm clusters nodes into different communities based on Euclidean distance

.. py:function:: gds.beta.kmeans.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.leiden.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    Leiden is a community detection algorithm, which guarantees that communities are well connected

.. py:function:: gds.beta.leiden.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.leiden.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    Executes the algorithm and returns result statistics without writing the result to Neo4j.

.. py:function:: gds.beta.leiden.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.leiden.stream(self, G: Graph, **config: Any) -> DataFrame

    Leiden is a community detection algorithm, which guarantees that communities are well connected

.. py:function:: gds.beta.leiden.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.leiden.write(self, G: Graph, **config: Any) -> "Series[Any]"

    Leiden is a community detection algorithm, which guarantees that communities are well connected

.. py:function:: gds.beta.leiden.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.modularityOptimization.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The Modularity Optimization algorithm groups the nodes in the graph by optimizing the graphs modularity.

.. py:function:: gds.beta.modularityOptimization.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.modularityOptimization.stream(self, G: Graph, **config: Any) -> DataFrame

    The Modularity Optimization algorithm groups the nodes in the graph by optimizing the graphs modularity.

.. py:function:: gds.beta.modularityOptimization.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.modularityOptimization.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The Modularity Optimization algorithm groups the nodes in the graph by optimizing the graphs modularity.

.. py:function:: gds.beta.modularityOptimization.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.scaleProperties.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    Scale node properties

.. py:function:: gds.beta.scaleProperties.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.


.. py:function:: gds.beta.scaleProperties.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    Scale node properties

.. py:function:: gds.beta.scaleProperties.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.scaleProperties.stream(self, G: Graph, **config: Any) -> DataFrame

    Scale node properties

.. py:function:: gds.beta.scaleProperties.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.scaleProperties.write(self, G: Graph, **config: Any) -> "Series[Any]"

    Scale node properties

.. py:function:: gds.beta.scaleProperties.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.spanningTree.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The spanning tree algorithm visits all nodes that are in the same connected component as the starting node,
    and returns a spanning tree of all nodes in the component where the total weight of the relationships is either minimized or maximized.

.. py:function:: gds.beta.spanningTree.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.spanningTree.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    The spanning tree algorithm visits all nodes that are in the same connected component as the starting node,
    and returns a spanning tree of all nodes in the component
    where the total weight of the relationships is either minimized or maximized.

.. py:function:: gds.beta.spanningTree.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.spanningTree.stream(self, G: Graph, **config: Any) -> DataFrame

    The spanning tree algorithm visits all nodes that are in the same connected component as the starting node,
    and returns a spanning tree of all nodes in the component
    where the total weight of the relationships is either minimized or maximized.

.. py:function:: gds.beta.spanningTree.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.spanningTree.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The spanning tree algorithm visits all nodes that are in the same connected component as the starting node,
    and returns a spanning tree of all nodes in the component
    where the total weight of the relationships is either minimized or maximized.

.. py:function:: gds.beta.spanningTree.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.steinerTree.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The steiner tree algorithm accepts a source node, as well as a list of target nodes.
    It then attempts to find a spanning tree where there is a path from the source node to each target node,
    such that the total weight of the relationships is as low as possible.

.. py:function:: gds.beta.steinerTree.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    The steiner tree algorithm accepts a source node, as well as a list of target nodes.
    It then attempts to find a spanning tree where there is a path from the source node to each target node,
    such that the total weight of the relationships is as low as possible.

.. py:function:: gds.beta.steinerTree.stream(self, G: Graph, **config: Any) -> DataFrame

    The steiner tree algorithm accepts a source node, as well as a list of target nodes.
    It then attempts to find a spanning tree where there is a path from the source node to each target node,
    such that the total weight of the relationships is as low as possible.

.. py:function:: gds.beta.steinerTree.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The steiner tree algorithm accepts a source node, as well as a list of target nodes.
    It then attempts to find a spanning tree where there is a path from the source node to each target node,
    such that the total weight of the relationships is as low as possible.

.. py:function:: gds.betweenness.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    Betweenness centrality measures the relative information flow that passes through a node.

.. py:function:: gds.betweenness.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Betweenness centrality measures the relative information flow that passes through a node.

.. py:function:: gds.betweenness.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    Betweenness centrality measures the relative information flow that passes through a node.

.. py:function:: gds.betweenness.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Betweenness centrality measures the relative information flow that passes through a node.

.. py:function:: gds.betweenness.stream(self, G: Graph, **config: Any) -> DataFrame

    Betweenness centrality measures the relative information flow that passes through a node.

.. py:function:: gds.betweenness.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Betweenness centrality measures the relative information flow that passes through a node.

.. py:function:: gds.betweenness.write(self, G: Graph, **config: Any) -> "Series[Any]"

    Betweenness centrality measures the relative information flow that passes through a node.

.. py:function:: gds.betweenness.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Betweenness centrality measures the relative information flow that passes through a node.

.. py:function:: gds.bfs.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    BFS is a traversal algorithm, which explores all of the neighbor nodes at the present depth
    prior to moving on to the nodes at the next depth level.

.. py:function:: gds.bfs.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.bfs.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    BFS is a traversal algorithm, which explores all of the neighbor nodes at the present depth
    prior to moving on to the nodes at the next depth level.

.. py:function:: gds.bfs.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.bfs.stream(self, G: Graph, **config: Any) -> DataFrame

    BFS is a traversal algorithm, which explores all of the neighbor nodes at the present depth
    prior to moving on to the nodes at the next depth level.

.. py:function:: gds.bfs.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"
    
    BFS is a traversal algorithm, which explores all of the neighbor nodes at the present depth
    prior to moving on to the nodes at the next depth level."""

.. py:function:: gds.degree.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    Degree centrality measures the number of incoming and outgoing relationships from a node.

.. py:function:: gds.degree.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Degree centrality measures the number of incoming and outgoing relationships from a node.

.. py:function:: gds.degree.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    Degree centrality measures the number of incoming and outgoing relationships from a node.

.. py:function:: gds.degree.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Degree centrality measures the number of incoming and outgoing relationships from a node.

.. py:function:: gds.degree.stream(self, G: Graph, **config: Any) -> DataFrame

    Degree centrality measures the number of incoming and outgoing relationships from a node.

.. py:function:: gds.degree.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Degree centrality measures the number of incoming and outgoing relationships from a node.

.. py:function:: gds.degree.write(self, G: Graph, **config: Any) -> "Series[Any]"

    Degree centrality measures the number of incoming and outgoing relationships from a node.

.. py:function:: gds.degree.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Degree centrality measures the number of incoming and outgoing relationships from a node.

.. py:function:: gds.dfs.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    Depth-first search (DFS) is an algorithm for traversing or searching tree or graph data structures. 
    The algorithm starts at the root node (selecting some arbitrary node as the root node in the case of a graph) 
    and explores as far as possible along each branch before backtracking.

.. py:function:: gds.dfs.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.dfs.stream(self, G: Graph, **config: Any) -> DataFrame

    Depth-first search (DFS) is an algorithm for traversing or searching tree or graph data structures. 
    The algorithm starts at the root node (selecting some arbitrary node as the root node in the case of a graph) 
    and explores as far as possible along each branch before backtracking.

.. py:function:: gds.dfs.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Depth-first search (DFS) is an algorithm for traversing or searching tree or graph data structures. 
    The algorithm starts at the root node (selecting some arbitrary node as the root node in the case of a graph) 
    and explores as far as possible along each branch before backtracking.

.. py:function:: gds.eigenvector.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    Eigenvector Centrality is an algorithm that measures the transitive influence or connectivity of nodes.

.. py:function:: gds.eigenvector.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.eigenvector.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    Eigenvector Centrality is an algorithm that measures the transitive influence or connectivity of nodes.

.. py:function:: gds.eigenvector.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.eigenvector.stream(self, G: Graph, **config: Any) -> DataFrame

    Eigenvector Centrality is an algorithm that measures the transitive influence or connectivity of nodes.

.. py:function:: gds.eigenvector.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.eigenvector.write(self, G: Graph, **config: Any) -> "Series[Any]"

    Eigenvector Centrality is an algorithm that measures the transitive influence or connectivity of nodes.

.. py:function:: gds.eigenvector.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.knn.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance 
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties

.. py:function:: gds.knn.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.knn.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance 
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties

.. py:function:: gds.knn.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.knn.stream(self, G: Graph, **config: Any) -> DataFrame

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance 
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties

.. py:function:: gds.knn.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.knn.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance 
    between two nodes is among the k nearest distances compared to other nodes.
    KNN computes distances based on the similarity of node properties

.. py:function:: gds.knn.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.labelPropagation.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The Label Propagation algorithm is a fast algorithm for finding communities in a graph.

.. py:function:: gds.labelPropagation.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.labelPropagation.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    The Label Propagation algorithm is a fast algorithm for finding communities in a graph.

.. py:function:: gds.labelPropagation.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.labelPropagation.stream(self, G: Graph, **config: Any) -> DataFrame

    The Label Propagation algorithm is a fast algorithm for finding communities in a graph.

.. py:function:: gds.labelPropagation.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.labelPropagation.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The Label Propagation algorithm is a fast algorithm for finding communities in a graph.

.. py:function:: gds.labelPropagation.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.localClusteringCoefficient.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The local clustering coefficient is a metric quantifying how connected the neighborhood of a node is.

.. py:function:: gds.localClusteringCoefficient.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.localClusteringCoefficient.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    Executes the algorithm and returns result statistics without writing the result to Neo4j.

.. py:function:: gds.localClusteringCoefficient.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.localClusteringCoefficient.stream(self, G: Graph, **config: Any) -> DataFrame

    The local clustering coefficient is a metric quantifying how connected the neighborhood of a node is.

.. py:function:: gds.localClusteringCoefficient.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.localClusteringCoefficient.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The local clustering coefficient is a metric quantifying how connected the neighborhood of a node is.

.. py:function:: gds.localClusteringCoefficient.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.louvain.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The Louvain method for community detection is an algorithm for detecting communities in networks.

.. py:function:: gds.louvain.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.louvain.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    Executes the algorithm and returns result statistics without writing the result to Neo4j.

.. py:function:: gds.louvain.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.louvain.stream(self, G: Graph, **config: Any) -> DataFrame

    The Louvain method for community detection is an algorithm for detecting communities in networks.

.. py:function:: gds.louvain.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.louvain.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The Louvain method for community detection is an algorithm for detecting communities in networks.

.. py:function:: gds.louvain.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.nodeSimilarity.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The Node Similarity algorithm compares a set of nodes based on the nodes they are connected to. 
    Two nodes are considered similar if they share many of the same neighbors. 
    Node Similarity computes pair-wise similarities based on the Jaccard metric.

.. py:function:: gds.nodeSimilarity.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.nodeSimilarity.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    The Node Similarity algorithm compares a set of nodes based on the nodes they are connected to. 
    Two nodes are considered similar if they share many of the same neighbors. 
    Node Similarity computes pair-wise similarities based on the Jaccard metric.

.. py:function:: gds.nodeSimilarity.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.nodeSimilarity.stream(self, G: Graph, **config: Any) -> DataFrame

    The Node Similarity algorithm compares a set of nodes based on the nodes they are connected to. 
    Two nodes are considered similar if they share many of the same neighbors. 
    Node Similarity computes pair-wise similarities based on the Jaccard metric.

.. py:function:: gds.nodeSimilarity.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.nodeSimilarity.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The Node Similarity algorithm compares a set of nodes based on the nodes they are connected to. 
    Two nodes are considered similar if they share many of the same neighbors. 
    Node Similarity computes pair-wise similarities based on the Jaccard metric.

.. py:function:: gds.nodeSimilarity.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.pageRank.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

	Page Rank is an algorithm that measures the transitive influence or connectivity of nodes.

.. py:function:: gds.pageRank.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.pageRank.stats(self, G: Graph, **config: Any) -> "Series[Any]"

	Executes the algorithm and returns result statistics without writing the result to Neo4j.

.. py:function:: gds.pageRank.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.pageRank.stream(self, G: Graph, **config: Any) -> DataFrame

	Page Rank is an algorithm that measures the transitive influence or connectivity of nodes.

.. py:function:: gds.pageRank.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.pageRank.write(self, G: Graph, **config: Any) -> "Series[Any]"

	Page Rank is an algorithm that measures the transitive influence or connectivity of nodes.

.. py:function:: gds.pageRank.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.randomWalk.stats(self, G: Graph, **config: Any) -> "Series[Any]"

	Random Walk is an algorithm that provides random paths in a graph. It’s similar to how a drunk person traverses a city.

.. py:function:: gds.randomWalk.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.randomWalk.stream(self, G: Graph, **config: Any) -> DataFrame

	Random Walk is an algorithm that provides random paths in a graph. It’s similar to how a drunk person traverses a city.

.. py:function:: gds.randomWalk.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.shortestPath.astar.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The A* shortest path algorithm computes the shortest path between a pair of nodes. It uses the relationship weight
    property to compare path lengths. In addition,
    this implementation uses the haversine distance as a heuristic to converge faster.

.. py:function:: gds.shortestPath.astar.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.shortestPath.astar.stream(self, G: Graph, **config: Any) -> DataFrame

    The A* shortest path algorithm computes the shortest path between a pair of nodes. It uses the relationship weight
    property to compare path lengths. In addition,
    this implementation uses the haversine distance as a heuristic to converge faster.

.. py:function:: gds.shortestPath.astar.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.shortestPath.astar.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The A* shortest path algorithm computes the shortest path between a pair of nodes. It uses the relationship weight
    property to compare path lengths. In addition,
    this implementation uses the haversine distance as a heuristic to converge faster.

.. py:function:: gds.shortestPath.astar.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.shortestPath.dijkstra.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

	The Dijkstra shortest path algorithm computes the shortest (weighted) path between a pair of nodes.

.. py:function:: gds.shortestPath.dijkstra.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.shortestPath.dijkstra.stream(self, G: Graph, **config: Any) -> DataFrame

	The Dijkstra shortest path algorithm computes the shortest (weighted) path between a pair of nodes.

.. py:function:: gds.shortestPath.dijkstra.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.shortestPath.dijkstra.write(self, G: Graph, **config: Any) -> "Series[Any]"

	The Dijkstra shortest path algorithm computes the shortest (weighted) path between a pair of nodes.

.. py:function:: gds.shortestPath.dijkstra.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.shortestPath.yens.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

	The Yen's shortest path algorithm computes the k shortest (weighted) paths between a pair of nodes.

.. py:function:: gds.shortestPath.yens.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.shortestPath.yens.stream(self, G: Graph, **config: Any) -> DataFrame

	The Yen's shortest path algorithm computes the k shortest (weighted) paths between a pair of nodes.

.. py:function:: gds.shortestPath.yens.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.shortestPath.yens.write(self, G: Graph, **config: Any) -> "Series[Any]"

	The Yen's shortest path algorithm computes the k shortest (weighted) paths between a pair of nodes.

.. py:function:: gds.shortestPath.yens.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.triangleCount.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    Triangle counting is a community detection graph algorithm that is used to
    determine the number of triangles passing through each node in the graph.

.. py:function:: gds.triangleCount.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.triangleCount.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    Triangle counting is a community detection graph algorithm that is used to
    determine the number of triangles passing through each node in the graph.

.. py:function:: gds.triangleCount.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.triangleCount.stream(self, G: Graph, **config: Any) -> DataFrame

    Triangle counting is a community detection graph algorithm that is used to
    determine the number of triangles passing through each node in the graph.

.. py:function:: gds.triangleCount.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.triangleCount.write(self, G: Graph, **config: Any) -> "Series[Any]"

    Triangle counting is a community detection graph algorithm that is used to
    determine the number of triangles passing through each node in the graph.

.. py:function:: gds.triangleCount.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Triangle counting is a community detection graph algorithm that is used to
    determine the number of triangles passing through each node in the graph.

.. py:function:: gds.wcc.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The WCC algorithm finds sets of connected nodes in an undirected graph,
    where all nodes in the same set form a connected component.

.. py:function:: gds.wcc.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.wcc.stats(self, G: Graph, **config: Any) -> "Series[Any]"

	Executes the algorithm and returns result statistics without writing the result to Neo4j.

.. py:function:: gds.wcc.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.wcc.stream(self, G: Graph, **config: Any) -> DataFrame

    The WCC algorithm finds sets of connected nodes in an undirected graph,
    where all nodes in the same set form a connected component.

.. py:function:: gds.wcc.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.wcc.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The WCC algorithm finds sets of connected nodes in an undirected graph,
    where all nodes in the same set form a connected component.

.. py:function:: gds.wcc.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

	Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.alpha.linkprediction.adamicAdar(self, node1: int, node2: int, **config: Any) -> float

    Given two nodes, calculate Adamic Adar similarity

.. py:function:: gds.alpha.linkprediction.commonNeighbors(self, node1: int, node2: int, **config: Any) -> float

    Given two nodes, returns the number of common neighbors

.. py:function:: gds.alpha.linkprediction.preferentialAttachment(self, node1: int, node2: int, **config: Any) -> float

    Given two nodes, calculate Preferential Attachment

.. py:function:: gds.alpha.linkprediction.resourceAllocation(self, node1: int, node2: int, **config: Any) -> float

    Given two nodes, calculate Resource Allocation similarity

.. py:function:: gds.alpha.linkprediction.sameCommunity(self, node1: int, node2: int, communityProperty: Optional[str] = None) -> float

    Given two nodes, indicates if they have the same community

.. py:function:: gds.alpha.linkprediction.totalNeighbors(self, node1: int, node2: int, **config: Any) -> float

    Given two nodes, calculate Total Neighbors
