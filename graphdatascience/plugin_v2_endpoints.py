from graphdatascience.arrow_client.v1.gds_arrow_client import GdsArrowClient
from graphdatascience.procedure_surface.api.catalog.scale_properties_endpoints import ScalePropertiesEndpoints
from graphdatascience.procedure_surface.api.centrality.articlerank_endpoints import ArticleRankEndpoints
from graphdatascience.procedure_surface.api.centrality.articulationpoints_endpoints import ArticulationPointsEndpoints
from graphdatascience.procedure_surface.api.centrality.betweenness_endpoints import BetweennessEndpoints
from graphdatascience.procedure_surface.api.centrality.celf_endpoints import CelfEndpoints
from graphdatascience.procedure_surface.api.centrality.closeness_endpoints import ClosenessEndpoints
from graphdatascience.procedure_surface.api.centrality.closeness_harmonic_endpoints import ClosenessHarmonicEndpoints
from graphdatascience.procedure_surface.api.centrality.degree_endpoints import DegreeEndpoints
from graphdatascience.procedure_surface.api.centrality.eigenvector_endpoints import EigenvectorEndpoints
from graphdatascience.procedure_surface.api.centrality.pagerank_endpoints import PageRankEndpoints
from graphdatascience.procedure_surface.api.community.clique_counting_endpoints import CliqueCountingEndpoints
from graphdatascience.procedure_surface.api.community.conductance_endpoints import ConductanceEndpoints
from graphdatascience.procedure_surface.api.community.hdbscan_endpoints import HdbscanEndpoints
from graphdatascience.procedure_surface.api.community.k1coloring_endpoints import K1ColoringEndpoints
from graphdatascience.procedure_surface.api.community.kcore_endpoints import KCoreEndpoints
from graphdatascience.procedure_surface.api.community.kmeans_endpoints import KMeansEndpoints
from graphdatascience.procedure_surface.api.community.labelpropagation_endpoints import LabelPropagationEndpoints
from graphdatascience.procedure_surface.api.community.leiden_endpoints import LeidenEndpoints
from graphdatascience.procedure_surface.api.community.local_clustering_coefficient_endpoints import (
    LocalClusteringCoefficientEndpoints,
)
from graphdatascience.procedure_surface.api.community.louvain_endpoints import LouvainEndpoints
from graphdatascience.procedure_surface.api.community.maxkcut_endpoints import MaxKCutEndpoints
from graphdatascience.procedure_surface.api.community.modularity_optimization_endpoints import (
    ModularityOptimizationEndpoints,
)
from graphdatascience.procedure_surface.api.community.scc_endpoints import SccEndpoints
from graphdatascience.procedure_surface.api.community.sllpa_endpoints import SllpaEndpoints
from graphdatascience.procedure_surface.api.community.triangle_count_endpoints import TriangleCountEndpoints
from graphdatascience.procedure_surface.api.community.wcc_endpoints import WccEndpoints
from graphdatascience.procedure_surface.api.config_endpoints import ConfigEndpoints
from graphdatascience.procedure_surface.api.node_embedding.fastrp_endpoints import FastRPEndpoints
from graphdatascience.procedure_surface.api.node_embedding.graphsage_endpoints import GraphSageEndpoints
from graphdatascience.procedure_surface.api.node_embedding.hashgnn_endpoints import HashGNNEndpoints
from graphdatascience.procedure_surface.api.node_embedding.node2vec_endpoints import Node2VecEndpoints
from graphdatascience.procedure_surface.api.pathfinding.all_shortest_path_endpoints import AllShortestPathEndpoints
from graphdatascience.procedure_surface.api.pathfinding.k_spanning_tree_endpoints import KSpanningTreeEndpoints
from graphdatascience.procedure_surface.api.pathfinding.max_flow_endpoints import MaxFlowEndpoints
from graphdatascience.procedure_surface.api.pathfinding.prize_steiner_tree_endpoints import PrizeSteinerTreeEndpoints
from graphdatascience.procedure_surface.api.pathfinding.shortest_path_endpoints import ShortestPathEndpoints
from graphdatascience.procedure_surface.api.pathfinding.single_source_bellman_ford_endpoints import (
    SingleSourceBellmanFordEndpoints,
)
from graphdatascience.procedure_surface.api.pathfinding.spanning_tree_endpoints import SpanningTreeEndpoints
from graphdatascience.procedure_surface.api.pathfinding.steiner_tree_endpoints import SteinerTreeEndpoints
from graphdatascience.procedure_surface.api.similarity.knn_endpoints import KnnEndpoints
from graphdatascience.procedure_surface.api.similarity.node_similarity_endpoints import NodeSimilarityEndpoints
from graphdatascience.procedure_surface.api.system_endpoints import SystemEndpoints
from graphdatascience.procedure_surface.cypher.catalog.scale_properties_cypher_endpoints import (
    ScalePropertiesCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.catalog_cypher_endpoints import CatalogCypherEndpoints
from graphdatascience.procedure_surface.cypher.centrality.articlerank_cypher_endpoints import ArticleRankCypherEndpoints
from graphdatascience.procedure_surface.cypher.centrality.articulationpoints_cypher_endpoints import (
    ArticulationPointsCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.centrality.betweenness_cypher_endpoints import BetweennessCypherEndpoints
from graphdatascience.procedure_surface.cypher.centrality.celf_cypher_endpoints import CelfCypherEndpoints
from graphdatascience.procedure_surface.cypher.centrality.closeness_cypher_endpoints import ClosenessCypherEndpoints
from graphdatascience.procedure_surface.cypher.centrality.closeness_harmonic_cypher_endpoints import (
    ClosenessHarmonicCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.centrality.degree_cypher_endpoints import DegreeCypherEndpoints
from graphdatascience.procedure_surface.cypher.centrality.eigenvector_cypher_endpoints import EigenvectorCypherEndpoints
from graphdatascience.procedure_surface.cypher.centrality.pagerank_cypher_endpoints import PageRankCypherEndpoints
from graphdatascience.procedure_surface.cypher.community.clique_counting_cypher_endpoints import (
    CliqueCountingCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.community.conductance_cypher_endpoints import ConductanceCypherEndpoints
from graphdatascience.procedure_surface.cypher.community.hdbscan_cypher_endpoints import HdbscanCypherEndpoints
from graphdatascience.procedure_surface.cypher.community.k1coloring_cypher_endpoints import K1ColoringCypherEndpoints
from graphdatascience.procedure_surface.cypher.community.kcore_cypher_endpoints import KCoreCypherEndpoints
from graphdatascience.procedure_surface.cypher.community.kmeans_cypher_endpoints import KMeansCypherEndpoints
from graphdatascience.procedure_surface.cypher.community.labelpropagation_cypher_endpoints import (
    LabelPropagationCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.community.leiden_cypher_endpoints import LeidenCypherEndpoints
from graphdatascience.procedure_surface.cypher.community.local_clustering_coefficient_cypher_endpoints import (
    LocalClusteringCoefficientCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.community.louvain_cypher_endpoints import LouvainCypherEndpoints
from graphdatascience.procedure_surface.cypher.community.maxkcut_cypher_endpoints import MaxKCutCypherEndpoints
from graphdatascience.procedure_surface.cypher.community.modularity_optimization_cypher_endpoints import (
    ModularityOptimizationCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.community.scc_cypher_endpoints import SccCypherEndpoints
from graphdatascience.procedure_surface.cypher.community.sllpa_cypher_endpoints import SllpaCypherEndpoints
from graphdatascience.procedure_surface.cypher.community.triangle_count_cypher_endpoints import (
    TriangleCountCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.community.wcc_cypher_endpoints import WccCypherEndpoints
from graphdatascience.procedure_surface.cypher.config_cypher_endpoints import ConfigCypherEndpoints
from graphdatascience.procedure_surface.cypher.node_embedding.fastrp_cypher_endpoints import FastRPCypherEndpoints
from graphdatascience.procedure_surface.cypher.node_embedding.graphsage_predict_cypher_endpoints import (
    GraphSagePredictCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.node_embedding.graphsage_train_cypher_endpoints import (
    GraphSageTrainCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.node_embedding.hashgnn_cypher_endpoints import HashGNNCypherEndpoints
from graphdatascience.procedure_surface.cypher.node_embedding.node2vec_cypher_endpoints import Node2VecCypherEndpoints
from graphdatascience.procedure_surface.cypher.pathfinding.all_shortest_path_cypher_endpoints import (
    AllShortestPathCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pathfinding.k_spanning_tree_cypher_endpoints import (
    KSpanningTreeCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pathfinding.max_flow_cypher_endpoints import MaxFlowCypherEndpoints
from graphdatascience.procedure_surface.cypher.pathfinding.prize_steiner_tree_cypher_endpoints import (
    PrizeSteinerTreeCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pathfinding.shortest_path_cypher_endpoints import (
    ShortestPathCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pathfinding.single_source_bellman_ford_cypher_endpoints import (
    BellmanFordCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pathfinding.spanning_tree_cypher_endpoints import (
    SpanningTreeCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pathfinding.steiner_tree_cypher_endpoints import (
    SteinerTreeCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.similarity.knn_cypher_endpoints import KnnCypherEndpoints
from graphdatascience.procedure_surface.cypher.similarity.node_similarity_cypher_endpoints import (
    NodeSimilarityCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.system_cypher_endpoints import SystemCypherEndpoints
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


class PluginV2Endpoints:
    def __init__(self, db_client: Neo4jQueryRunner, arrow_client: GdsArrowClient | None) -> None:
        self._db_client = db_client
        self._arrow_client = arrow_client

    def set_progress(self, show_progress: bool) -> None:
        self._db_client.set_show_progress(show_progress)

    @property
    def graph(self) -> CatalogCypherEndpoints:
        """
        Return endpoints for graph management.
        """
        return CatalogCypherEndpoints(self._db_client, self._arrow_client)

    @property
    def config(self) -> ConfigEndpoints:
        """
        Return endpoints for configuration.
        """
        return ConfigCypherEndpoints(self._db_client)

    @property
    def system(self) -> SystemEndpoints:
        """
        Return endpoints for system management.
        """
        return SystemCypherEndpoints(self._db_client)

    ## Algorithms

    @property
    def all_shortest_paths(self) -> AllShortestPathEndpoints:
        """
        Return endpoints for the all shortest paths algorithm.
        """
        return AllShortestPathCypherEndpoints(self._db_client)

    @property
    def article_rank(self) -> ArticleRankEndpoints:
        """
        Return endpoints for the article rank algorithm.
        """
        return ArticleRankCypherEndpoints(self._db_client)

    @property
    def articulation_points(self) -> ArticulationPointsEndpoints:
        """
        Return endpoints for the articulation points algorithm.
        """
        return ArticulationPointsCypherEndpoints(self._db_client)

    @property
    def betweenness_centrality(self) -> BetweennessEndpoints:
        """
        Return endpoints for the betweenness centrality algorithm.
        """
        return BetweennessCypherEndpoints(self._db_client)

    @property
    def bellman_ford(self) -> SingleSourceBellmanFordEndpoints:
        """
        Return endpoints for the single source Bellman-Ford algorithm.
        """
        return BellmanFordCypherEndpoints(self._db_client)

    @property
    def clique_counting(self) -> CliqueCountingEndpoints:
        """
        Return endpoints for the clique counting algorithm.
        """
        return CliqueCountingCypherEndpoints(self._db_client)

    @property
    def conductance(self) -> ConductanceEndpoints:
        """
        Return endpoints for the conductance algorithm.
        """
        return ConductanceCypherEndpoints(self._db_client)

    @property
    def closeness_centrality(self) -> ClosenessEndpoints:
        """
        Return endpoints for the closeness centrality algorithm.
        """
        return ClosenessCypherEndpoints(self._db_client)

    @property
    def degree_centrality(self) -> DegreeEndpoints:
        """
        Return endpoints for the degree centrality algorithm.
        """
        return DegreeCypherEndpoints(self._db_client)

    @property
    def eigenvector_centrality(self) -> EigenvectorEndpoints:
        """
        Return endpoints for the eigenvector centrality algorithm.
        """
        return EigenvectorCypherEndpoints(self._db_client)

    @property
    def fast_rp(self) -> FastRPEndpoints:
        """
        Return endpoints for the fast RP algorithm.
        """
        return FastRPCypherEndpoints(self._db_client)

    @property
    def graph_sage(self) -> GraphSageEndpoints:
        """
        Return endpoints for the GraphSage algorithm.
        """
        return GraphSageEndpoints(
            train_endpoints=GraphSageTrainCypherEndpoints(self._db_client),
            predict_endpoints=GraphSagePredictCypherEndpoints(self._db_client),
        )

    @property
    def harmonic_centrality(self) -> ClosenessHarmonicEndpoints:
        """
        Return endpoints for the harmonic centrality algorithm.
        """
        return ClosenessHarmonicCypherEndpoints(self._db_client)

    @property
    def hash_gnn(self) -> HashGNNEndpoints:
        """
        Return endpoints for the HashGNN algorithm.
        """
        return HashGNNCypherEndpoints(self._db_client)

    @property
    def hdbscan(self) -> HdbscanEndpoints:
        """
        Return endpoints for the HDBSCAN algorithm.
        """
        return HdbscanCypherEndpoints(self._db_client)

    @property
    def influence_maximization_celf(self) -> CelfEndpoints:
        """
        Return endpoints for the influence maximization CELF algorithm.
        """
        return CelfCypherEndpoints(self._db_client)

    @property
    def k1_coloring(self) -> K1ColoringEndpoints:
        """
        Return endpoints for the K1 coloring algorithm.
        """
        return K1ColoringCypherEndpoints(self._db_client)

    @property
    def k_core_decomposition(self) -> KCoreEndpoints:
        """
        Return endpoints for the K-core decomposition algorithm.
        """
        return KCoreCypherEndpoints(self._db_client)

    @property
    def kmeans(self) -> KMeansEndpoints:
        """
        Return endpoints for the K-means algorithm.
        """
        return KMeansCypherEndpoints(self._db_client)

    @property
    def knn(self) -> KnnEndpoints:
        """
        Return endpoints for the K-nearest neighbors algorithm.
        """
        return KnnCypherEndpoints(self._db_client)

    @property
    def k_spanning_tree(self) -> KSpanningTreeEndpoints:
        """
        Return endpoints for the K-spanning tree algorithm.
        """
        return KSpanningTreeCypherEndpoints(self._db_client)

    @property
    def label_propagation(self) -> LabelPropagationEndpoints:
        """
        Return endpoints for the label propagation algorithm.
        """
        return LabelPropagationCypherEndpoints(self._db_client)

    @property
    def leiden(self) -> LeidenEndpoints:
        """
        Return endpoints for the Leiden algorithm.
        """
        return LeidenCypherEndpoints(self._db_client)

    @property
    def local_clustering_coefficient(self) -> LocalClusteringCoefficientEndpoints:
        """
        Return endpoints for the local clustering coefficient algorithm.
        """
        return LocalClusteringCoefficientCypherEndpoints(self._db_client)

    @property
    def louvain(self) -> LouvainEndpoints:
        """
        Return endpoints for the Louvain algorithm.
        """
        return LouvainCypherEndpoints(self._db_client)

    @property
    def max_flow(self) -> MaxFlowEndpoints:
        """
        Return endpoints for the Max Flow algorithm.
        """
        return MaxFlowCypherEndpoints(self._db_client)

    @property
    def max_k_cut(self) -> MaxKCutEndpoints:
        """
        Return endpoints for the Max K-cut algorithm.
        """
        return MaxKCutCypherEndpoints(self._db_client)

    @property
    def modularity_optimization(self) -> ModularityOptimizationEndpoints:
        """
        Return endpoints for the modularity optimization algorithm.
        """
        return ModularityOptimizationCypherEndpoints(self._db_client)

    @property
    def node2vec(self) -> Node2VecEndpoints:
        """
        Return endpoints for the Node2Vec algorithm.
        """
        return Node2VecCypherEndpoints(self._db_client)

    @property
    def node_similarity(self) -> NodeSimilarityEndpoints:
        """
        Return endpoints for the node similarity algorithm.
        """
        return NodeSimilarityCypherEndpoints(self._db_client)

    @property
    def page_rank(self) -> PageRankEndpoints:
        """
        Return endpoints for the PageRank algorithm.
        """
        return PageRankCypherEndpoints(self._db_client)

    @property
    def prize_steiner_tree(self) -> PrizeSteinerTreeEndpoints:
        """
        Return endpoints for the prize-collecting Steiner tree algorithm.
        """
        return PrizeSteinerTreeCypherEndpoints(self._db_client)

    @property
    def scc(self) -> SccEndpoints:
        """
        Return endpoints for the strongly connected components algorithm.
        """
        return SccCypherEndpoints(self._db_client)

    @property
    def scale_properties(self) -> ScalePropertiesEndpoints:
        """
        Return endpoints for scaling node properties.
        """
        return ScalePropertiesCypherEndpoints(self._db_client)

    @property
    def shortest_path(self) -> ShortestPathEndpoints:
        """
        Return endpoints for the shortest path algorithm.
        """
        return ShortestPathCypherEndpoints(self._db_client)

    @property
    def spanning_tree(self) -> SpanningTreeEndpoints:
        """
        Return endpoints for the spanning tree algorithm.
        """
        return SpanningTreeCypherEndpoints(self._db_client)

    @property
    def steiner_tree(self) -> SteinerTreeEndpoints:
        """
        Return endpoints for the Steiner tree algorithm.
        """
        return SteinerTreeCypherEndpoints(self._db_client)

    @property
    def sllpa(self) -> SllpaEndpoints:
        """
        Return endpoints for the speaker-listener label propagation algorithm.
        """
        return SllpaCypherEndpoints(self._db_client)

    @property
    def triangle_count(self) -> TriangleCountEndpoints:
        """
        Return endpoints for the triangle count algorithm.
        """
        return TriangleCountCypherEndpoints(self._db_client)

    @property
    def wcc(self) -> WccEndpoints:
        """
        Return endpoints for the weakly connected components algorithm.
        """
        return WccCypherEndpoints(self._db_client)
