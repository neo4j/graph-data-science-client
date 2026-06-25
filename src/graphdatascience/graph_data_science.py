from __future__ import annotations

import warnings
from types import TracebackType
from typing import Any, Type

import neo4j
from neo4j import Driver
from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.scale_properties_endpoints import ScalePropertiesEndpoints
from graphdatascience.procedure_surface.api.centrality.articlerank_endpoints import ArticleRankEndpoints
from graphdatascience.procedure_surface.api.centrality.articulationpoints_endpoints import ArticulationPointsEndpoints
from graphdatascience.procedure_surface.api.centrality.betweenness_endpoints import BetweennessEndpoints
from graphdatascience.procedure_surface.api.centrality.bridges_endpoints import BridgesEndpoints
from graphdatascience.procedure_surface.api.centrality.celf_endpoints import CelfEndpoints
from graphdatascience.procedure_surface.api.centrality.closeness_endpoints import ClosenessEndpoints
from graphdatascience.procedure_surface.api.centrality.closeness_harmonic_endpoints import ClosenessHarmonicEndpoints
from graphdatascience.procedure_surface.api.centrality.degree_endpoints import DegreeEndpoints
from graphdatascience.procedure_surface.api.centrality.eigenvector_endpoints import EigenvectorEndpoints
from graphdatascience.procedure_surface.api.centrality.pagerank_endpoints import PageRankEndpoints
from graphdatascience.procedure_surface.api.collapse_path_endpoints import CollapsePathEndpoints
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
from graphdatascience.procedure_surface.api.community.modularity_endpoints import ModularityEndpoints
from graphdatascience.procedure_surface.api.community.modularity_optimization_endpoints import (
    ModularityOptimizationEndpoints,
)
from graphdatascience.procedure_surface.api.community.scc_endpoints import SccEndpoints
from graphdatascience.procedure_surface.api.community.sllpa_endpoints import SllpaEndpoints
from graphdatascience.procedure_surface.api.community.triangle_count_endpoints import TriangleCountEndpoints
from graphdatascience.procedure_surface.api.community.triangles_endpoints import TrianglesEndpoints
from graphdatascience.procedure_surface.api.community.wcc_endpoints import WccEndpoints
from graphdatascience.procedure_surface.api.config_endpoints import ConfigEndpoints
from graphdatascience.procedure_surface.api.debug_endpoints import DebugEndpoints
from graphdatascience.procedure_surface.api.kge.kge_endpoints import KgeEndpoints
from graphdatascience.procedure_surface.api.license_endpoints import LicenseEndpoints
from graphdatascience.procedure_surface.api.model.model_catalog_endpoints import ModelCatalogEndpoints
from graphdatascience.procedure_surface.api.node_embedding.fastrp_endpoints import FastRPEndpoints
from graphdatascience.procedure_surface.api.node_embedding.graphsage_endpoints import GraphSageEndpoints
from graphdatascience.procedure_surface.api.node_embedding.hashgnn_endpoints import HashGNNEndpoints
from graphdatascience.procedure_surface.api.node_embedding.node2vec_endpoints import Node2VecEndpoints
from graphdatascience.procedure_surface.api.pathfinding.all_shortest_path_endpoints import AllShortestPathEndpoints
from graphdatascience.procedure_surface.api.pathfinding.bfs_endpoints import BFSEndpoints
from graphdatascience.procedure_surface.api.pathfinding.dag_endpoints import DagEndpoints
from graphdatascience.procedure_surface.api.pathfinding.dfs_endpoints import DFSEndpoints
from graphdatascience.procedure_surface.api.pathfinding.k_spanning_tree_endpoints import KSpanningTreeEndpoints
from graphdatascience.procedure_surface.api.pathfinding.max_flow_endpoints import MaxFlowEndpoints
from graphdatascience.procedure_surface.api.pathfinding.prize_steiner_tree_endpoints import PrizeSteinerTreeEndpoints
from graphdatascience.procedure_surface.api.pathfinding.random_walk_endpoints import RandomWalkEndpoints
from graphdatascience.procedure_surface.api.pathfinding.shortest_path_endpoints import ShortestPathEndpoints
from graphdatascience.procedure_surface.api.pathfinding.single_source_bellman_ford_endpoints import (
    SingleSourceBellmanFordEndpoints,
)
from graphdatascience.procedure_surface.api.pathfinding.spanning_tree_endpoints import SpanningTreeEndpoints
from graphdatascience.procedure_surface.api.pathfinding.steiner_tree_endpoints import SteinerTreeEndpoints
from graphdatascience.procedure_surface.api.pipeline import PipelineEndpoints
from graphdatascience.procedure_surface.api.similarity.knn_endpoints import KnnEndpoints
from graphdatascience.procedure_surface.api.similarity.node_similarity_endpoints import NodeSimilarityEndpoints
from graphdatascience.procedure_surface.api.similarity.similarity_functions import SimilarityFunctions
from graphdatascience.procedure_surface.api.topological_link_prediction_endpoints import (
    TopologicalLinkPredictionEndpoints,
)
from graphdatascience.procedure_surface.api.util_endpoints import UtilEndpoints
from graphdatascience.procedure_surface.cypher.catalog.catalog_cypher_endpoints import CatalogCypherEndpoints
from graphdatascience.procedure_surface.cypher.catalog.scale_properties_cypher_endpoints import (
    ScalePropertiesCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.centrality.articlerank_cypher_endpoints import ArticleRankCypherEndpoints
from graphdatascience.procedure_surface.cypher.centrality.articulationpoints_cypher_endpoints import (
    ArticulationPointsCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.centrality.betweenness_cypher_endpoints import BetweennessCypherEndpoints
from graphdatascience.procedure_surface.cypher.centrality.bridges_cypher_endpoints import BridgesCypherEndpoints
from graphdatascience.procedure_surface.cypher.centrality.celf_cypher_endpoints import CelfCypherEndpoints
from graphdatascience.procedure_surface.cypher.centrality.closeness_cypher_endpoints import ClosenessCypherEndpoints
from graphdatascience.procedure_surface.cypher.centrality.closeness_harmonic_cypher_endpoints import (
    ClosenessHarmonicCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.centrality.degree_cypher_endpoints import DegreeCypherEndpoints
from graphdatascience.procedure_surface.cypher.centrality.eigenvector_cypher_endpoints import EigenvectorCypherEndpoints
from graphdatascience.procedure_surface.cypher.centrality.pagerank_cypher_endpoints import PageRankCypherEndpoints
from graphdatascience.procedure_surface.cypher.collapse_path_cypher_endpoints import CollapsePathCypherEndpoints
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
from graphdatascience.procedure_surface.cypher.community.modularity_cypher_endpoints import ModularityCypherEndpoints
from graphdatascience.procedure_surface.cypher.community.modularity_optimization_cypher_endpoints import (
    ModularityOptimizationCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.community.scc_cypher_endpoints import SccCypherEndpoints
from graphdatascience.procedure_surface.cypher.community.sllpa_cypher_endpoints import SllpaCypherEndpoints
from graphdatascience.procedure_surface.cypher.community.triangle_count_cypher_endpoints import (
    TriangleCountCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.community.triangles_cypher_endpoints import TrianglesCypherEndpoints
from graphdatascience.procedure_surface.cypher.community.wcc_cypher_endpoints import WccCypherEndpoints
from graphdatascience.procedure_surface.cypher.config_cypher_endpoints import ConfigCypherEndpoints
from graphdatascience.procedure_surface.cypher.debug_cypher_endpoints import DebugCypherEndpoints
from graphdatascience.procedure_surface.cypher.kge.kge_predict_cypher_endpoints import KgePredictCypherEndpoints
from graphdatascience.procedure_surface.cypher.license_cypher_endpoints import LicenseCypherEndpoints
from graphdatascience.procedure_surface.cypher.list_progress_cypher_endpoint import ListProgressCypherEndpoint
from graphdatascience.procedure_surface.cypher.model.model_catalog_cypher_endpoints import (
    ModelCatalogCypherEndpoints,
)
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
from graphdatascience.procedure_surface.cypher.pathfinding.bfs_cypher_endpoints import BFSCypherEndpoints
from graphdatascience.procedure_surface.cypher.pathfinding.dag_cypher_endpoints import DagCypherEndpoints
from graphdatascience.procedure_surface.cypher.pathfinding.dfs_cypher_endpoints import DFSCypherEndpoints
from graphdatascience.procedure_surface.cypher.pathfinding.k_spanning_tree_cypher_endpoints import (
    KSpanningTreeCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pathfinding.max_flow_cypher_endpoints import MaxFlowCypherEndpoints
from graphdatascience.procedure_surface.cypher.pathfinding.prize_steiner_tree_cypher_endpoints import (
    PrizeSteinerTreeCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pathfinding.random_walk_cypher_endpoints import (
    RandomWalkCypherEndpoints,
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
from graphdatascience.procedure_surface.cypher.pipeline.pipeline_cypher_endpoints import (
    PipelineCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.similarity.knn_cypher_endpoints import KnnCypherEndpoints
from graphdatascience.procedure_surface.cypher.similarity.node_similarity_cypher_endpoints import (
    NodeSimilarityCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.topological_link_prediction_cypher_endpoints import (
    TopologicalLinkPredictionCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.util_cypher_endpoints import UtilCypherEndpoints
from graphdatascience.query_runner.query_mode import QueryMode

from .arrow_client.arrow_authentication import UsernamePasswordAuthentication
from .arrow_client.arrow_info import ArrowInfo
from .arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from .arrow_client.v1.gds_arrow_client import GdsArrowClient
from .query_runner.neo4j_query_runner import Neo4jQueryRunner
from .query_runner.query_runner import QueryRunner
from .query_runner.query_type import QueryType
from .server_version.server_version import ServerVersion
from .version import __min_server_version__


class GraphDataScience:
    """
    Primary API class for the Neo4j Graph Data Science Python Client.
    Always bind this object to a variable called `gds`.
    """

    def __init__(
        self,
        endpoint: str | Driver | QueryRunner,
        auth: tuple[str, str] | None = None,
        aura_ds: bool = False,
        database: str | None = None,
        arrow: str | bool = True,
        bookmarks: Any | None = None,
        show_progress: bool = True,
        arrow_client_options: dict[str, Any] | None = None,
    ):
        """
        Construct a new GraphDataScience object.

        Parameters
        ----------
        endpoint : str | Driver | QueryRunner
            The Neo4j endpoint to connect to. Most commonly, this is a Bolt connection URI.
        auth : tuple[str, str] | None, default None
            A username, password pair for database authentication.
        aura_ds : bool, default False
            A flag that indicates that that the client is used to connect
            to a Neo4j AuraDS instance.
        database: str | None, default None
            The Neo4j database to query against.
        arrow : str | bool, default True
            Arrow connection information. This is either a string or a bool.

            - If it is a string, it will be interpreted as a connection URL to a GDS Arrow Server.
            - If it is a bool:
                - True will make the client discover the connection URI to the GDS Arrow server via the Neo4j endpoint.
                - False will make the client use Bolt for all operations.
        bookmarks : Any | None, default None
            The Neo4j bookmarks to require a certain state before the next query gets executed.
        show_progress : bool, default True
            A flag to indicate whether to show progress bars for running procedures.
        arrow_client_options : dict[str, Any] | None, default None
            Additional options to be passed to the Arrow Flight client.
        """
        if aura_ds:
            GraphDataScience._validate_endpoint(endpoint)

        if isinstance(endpoint, QueryRunner):
            self._query_runner = endpoint
        else:
            db_auth: neo4j.Auth | None = None
            if auth:
                db_auth = neo4j.basic_auth(*auth)

            self._query_runner = Neo4jQueryRunner.create_for_db(
                endpoint, db_auth, aura_ds, database, bookmarks, show_progress
            )

        self._server_version = self._query_runner.server_version()

        if self._server_version < ServerVersion.from_string(__min_server_version__):
            warnings.warn(
                DeprecationWarning(
                    f"Client does not support the given server version `{self._server_version}`."
                    + " We recommend to either update the GDS server version or use a compatible version of the `graphdatascience` package."
                    + " Please refer to the compatibility matrix at https://neo4j.com/docs/graph-data-science-client/current/installation/#python-client-system-requirements."
                )
            )

        self._arrow_client: GdsArrowClient | None = None

        arrow_info = ArrowInfo.create(self._query_runner)
        if arrow and arrow_info.enabled:
            arrow_auth = None
            if auth is not None:
                username, password = auth
                arrow_auth = UsernamePasswordAuthentication(username, password)
            if isinstance(endpoint, Neo4jQueryRunner):
                if neo4j_auth := endpoint.get_auth():
                    arrow_auth = UsernamePasswordAuthentication(neo4j_auth[0], neo4j_auth[1])

            if isinstance(endpoint, Driver) and not auth:
                warnings.warn(
                    "Falling back to use Cypher for GDS. To use Arrow, you must explicitly provide the `auth` parameter."
                )
            else:
                self._arrow_client = GdsArrowClient(
                    AuthenticatedArrowClient(
                        arrow_info.listenAddress,
                        auth=arrow_auth,
                        encrypted=self._query_runner.encrypted(),
                        arrow_client_options=arrow_client_options,
                    )
                )

        self._query_runner.set_show_progress(show_progress)

    @property
    def graph(self) -> CatalogCypherEndpoints:
        """
        Return endpoints for graph management.
        """
        return CatalogCypherEndpoints(self._query_runner, self._arrow_client)

    @property
    def model(self) -> ModelCatalogEndpoints:
        """
        Return model-related endpoints for model management.
        """
        return ModelCatalogCypherEndpoints(self._query_runner)

    @property
    def config(self) -> ConfigEndpoints:
        """
        Return endpoints for configuration.
        """
        return ConfigCypherEndpoints(self._query_runner)

    @property
    def util(self) -> UtilEndpoints:
        """
        Return utility endpoints.
        """
        return UtilCypherEndpoints(self._query_runner)

    @property
    def license(self) -> LicenseEndpoints:
        """
        Return license endpoints.
        """
        return LicenseCypherEndpoints(self._query_runner)

    @property
    def debug(self) -> DebugEndpoints:
        """
        Return debug endpoints.
        """
        return DebugCypherEndpoints(self._query_runner)

    @property
    def list_progress(self) -> ListProgressCypherEndpoint:
        """
        Return endpoint for listing progress.
        """
        return ListProgressCypherEndpoint(self._query_runner)

    @property
    def collapse_path(self) -> CollapsePathEndpoints:
        """
        Return endpoints for collapsing relationship paths.
        """
        return CollapsePathCypherEndpoints(self._query_runner)

    @property
    def topological_link_prediction(self) -> TopologicalLinkPredictionEndpoints:
        """
        Return endpoints for topological link prediction functions.
        """
        return TopologicalLinkPredictionCypherEndpoints(self._query_runner)

    ## Algorithms

    @property
    def all_shortest_paths(self) -> AllShortestPathEndpoints:
        """
        Return endpoints for the all shortest paths algorithm.
        """
        return AllShortestPathCypherEndpoints(self._query_runner)

    @property
    def article_rank(self) -> ArticleRankEndpoints:
        """
        Return endpoints for the article rank algorithm.
        """
        return ArticleRankCypherEndpoints(self._query_runner)

    @property
    def bfs(self) -> BFSEndpoints:
        """
        Return endpoints for the Breadth First Search (BFS) algorithm.
        """
        return BFSCypherEndpoints(self._query_runner)

    @property
    def dfs(self) -> DFSEndpoints:
        """
        Return endpoints for the Depth First Search (DFS) algorithm.
        """
        return DFSCypherEndpoints(self._query_runner)

    @property
    def articulation_points(self) -> ArticulationPointsEndpoints:
        """
        Return endpoints for the articulation points algorithm.
        """
        return ArticulationPointsCypherEndpoints(self._query_runner)

    @property
    def betweenness_centrality(self) -> BetweennessEndpoints:
        """
        Return endpoints for the betweenness centrality algorithm.
        """
        return BetweennessCypherEndpoints(self._query_runner)

    @property
    def bridges(self) -> BridgesEndpoints:
        """
        Return endpoints for the bridges algorithm.
        """
        return BridgesCypherEndpoints(self._query_runner)

    @property
    def bellman_ford(self) -> SingleSourceBellmanFordEndpoints:
        """
        Return endpoints for the single source Bellman-Ford algorithm.
        """
        return BellmanFordCypherEndpoints(self._query_runner)

    @property
    def clique_counting(self) -> CliqueCountingEndpoints:
        """
        Return endpoints for the clique counting algorithm.
        """
        return CliqueCountingCypherEndpoints(self._query_runner)

    @property
    def conductance(self) -> ConductanceEndpoints:
        """
        Return endpoints for the conductance algorithm.
        """
        return ConductanceCypherEndpoints(self._query_runner)

    @property
    def closeness_centrality(self) -> ClosenessEndpoints:
        """
        Return endpoints for the closeness centrality algorithm.
        """
        return ClosenessCypherEndpoints(self._query_runner)

    @property
    def dag(self) -> DagEndpoints:
        """
        Return endpoints for Directed Acyclic Graph (DAG) algorithms.
        """
        return DagCypherEndpoints(self._query_runner)

    @property
    def degree_centrality(self) -> DegreeEndpoints:
        """
        Return endpoints for the degree centrality algorithm.
        """
        return DegreeCypherEndpoints(self._query_runner)

    @property
    def eigenvector_centrality(self) -> EigenvectorEndpoints:
        """
        Return endpoints for the eigenvector centrality algorithm.
        """
        return EigenvectorCypherEndpoints(self._query_runner)

    @property
    def fast_rp(self) -> FastRPEndpoints:
        """
        Return endpoints for the fast RP algorithm.
        """
        return FastRPCypherEndpoints(self._query_runner)

    @property
    def graph_sage(self) -> GraphSageEndpoints:
        """
        Return endpoints for the GraphSage algorithm.
        """
        return GraphSageEndpoints(
            train_endpoints=GraphSageTrainCypherEndpoints(self._query_runner),
            predict_endpoints=GraphSagePredictCypherEndpoints(self._query_runner),
        )

    @property
    def kge(self) -> KgeEndpoints:
        """
        Return endpoints for KGE (TransE/DistMult) relationship prediction.
        """
        return KgeEndpoints(KgePredictCypherEndpoints(self._query_runner))

    @property
    def harmonic_centrality(self) -> ClosenessHarmonicEndpoints:
        """
        Return endpoints for the harmonic centrality algorithm.
        """
        return ClosenessHarmonicCypherEndpoints(self._query_runner)

    @property
    def hash_gnn(self) -> HashGNNEndpoints:
        """
        Return endpoints for the HashGNN algorithm.
        """
        return HashGNNCypherEndpoints(self._query_runner)

    @property
    def hdbscan(self) -> HdbscanEndpoints:
        """
        Return endpoints for the HDBSCAN algorithm.
        """
        return HdbscanCypherEndpoints(self._query_runner)

    @property
    def influence_maximization_celf(self) -> CelfEndpoints:
        """
        Return endpoints for the influence maximization CELF algorithm.
        """
        return CelfCypherEndpoints(self._query_runner)

    @property
    def k1_coloring(self) -> K1ColoringEndpoints:
        """
        Return endpoints for the K1 coloring algorithm.
        """
        return K1ColoringCypherEndpoints(self._query_runner)

    @property
    def k_core_decomposition(self) -> KCoreEndpoints:
        """
        Return endpoints for the K-core decomposition algorithm.
        """
        return KCoreCypherEndpoints(self._query_runner)

    @property
    def kmeans(self) -> KMeansEndpoints:
        """
        Return endpoints for the K-means algorithm.
        """
        return KMeansCypherEndpoints(self._query_runner)

    @property
    def knn(self) -> KnnEndpoints:
        """
        Return endpoints for the K-nearest neighbors algorithm.
        """
        return KnnCypherEndpoints(self._query_runner)

    @property
    def k_spanning_tree(self) -> KSpanningTreeEndpoints:
        """
        Return endpoints for the K-spanning tree algorithm.
        """
        return KSpanningTreeCypherEndpoints(self._query_runner)

    @property
    def label_propagation(self) -> LabelPropagationEndpoints:
        """
        Return endpoints for the label propagation algorithm.
        """
        return LabelPropagationCypherEndpoints(self._query_runner)

    @property
    def leiden(self) -> LeidenEndpoints:
        """
        Return endpoints for the Leiden algorithm.
        """
        return LeidenCypherEndpoints(self._query_runner)

    @property
    def local_clustering_coefficient(self) -> LocalClusteringCoefficientEndpoints:
        """
        Return endpoints for the local clustering coefficient algorithm.
        """
        return LocalClusteringCoefficientCypherEndpoints(self._query_runner)

    @property
    def louvain(self) -> LouvainEndpoints:
        """
        Return endpoints for the Louvain algorithm.
        """
        return LouvainCypherEndpoints(self._query_runner)

    @property
    def max_flow(self) -> MaxFlowEndpoints:
        """
        Return endpoints for the Max Flow algorithm.
        """
        return MaxFlowCypherEndpoints(self._query_runner)

    @property
    def max_k_cut(self) -> MaxKCutEndpoints:
        """
        Return endpoints for the Max K-cut algorithm.
        """
        return MaxKCutCypherEndpoints(self._query_runner)

    @property
    def modularity(self) -> ModularityEndpoints:
        """
        Return endpoints for the modularity algorithm.
        """
        return ModularityCypherEndpoints(self._query_runner)

    @property
    def modularity_optimization(self) -> ModularityOptimizationEndpoints:
        """
        Return endpoints for the modularity optimization algorithm.
        """
        return ModularityOptimizationCypherEndpoints(self._query_runner)

    @property
    def node2vec(self) -> Node2VecEndpoints:
        """
        Return endpoints for the Node2Vec algorithm.
        """
        return Node2VecCypherEndpoints(self._query_runner)

    @property
    def node_similarity(self) -> NodeSimilarityEndpoints:
        """
        Return endpoints for the node similarity algorithm.
        """
        return NodeSimilarityCypherEndpoints(self._query_runner)

    @property
    def similarity(self) -> SimilarityFunctions:
        """
        Return similarity functions computed client-side.
        """
        return SimilarityFunctions()

    @property
    def page_rank(self) -> PageRankEndpoints:
        """
        Return endpoints for the PageRank algorithm.
        """
        return PageRankCypherEndpoints(self._query_runner)

    @property
    def prize_steiner_tree(self) -> PrizeSteinerTreeEndpoints:
        """
        Return endpoints for the prize-collecting Steiner tree algorithm.
        """
        return PrizeSteinerTreeCypherEndpoints(self._query_runner)

    @property
    def random_walk(self) -> RandomWalkEndpoints:
        """
        Return endpoints for the Random Walk algorithm.
        """
        return RandomWalkCypherEndpoints(self._query_runner)

    @property
    def scc(self) -> SccEndpoints:
        """
        Return endpoints for the strongly connected components algorithm.
        """
        return SccCypherEndpoints(self._query_runner)

    @property
    def scale_properties(self) -> ScalePropertiesEndpoints:
        """
        Return endpoints for scaling node properties.
        """
        return ScalePropertiesCypherEndpoints(self._query_runner)

    @property
    def shortest_path(self) -> ShortestPathEndpoints:
        """
        Return endpoints for the shortest path algorithm.
        """
        return ShortestPathCypherEndpoints(self._query_runner)

    @property
    def spanning_tree(self) -> SpanningTreeEndpoints:
        """
        Return endpoints for the spanning tree algorithm.
        """
        return SpanningTreeCypherEndpoints(self._query_runner)

    @property
    def steiner_tree(self) -> SteinerTreeEndpoints:
        """
        Return endpoints for the Steiner tree algorithm.
        """
        return SteinerTreeCypherEndpoints(self._query_runner)

    @property
    def sllpa(self) -> SllpaEndpoints:
        """
        Return endpoints for the speaker-listener label propagation algorithm.
        """
        return SllpaCypherEndpoints(self._query_runner)

    @property
    def triangle_count(self) -> TriangleCountEndpoints:
        """
        Return endpoints for the triangle count algorithm.
        """
        return TriangleCountCypherEndpoints(self._query_runner)

    @property
    def pipeline(self) -> PipelineEndpoints:
        """
        Return endpoints for pipeline procedures.
        """
        return PipelineCypherEndpoints(self._query_runner)

    @property
    def triangles(self) -> TrianglesEndpoints:
        """
        Return endpoint for the triangles algorithm.
        """
        return TrianglesCypherEndpoints(self._query_runner)

    @property
    def wcc(self) -> WccEndpoints:
        """
        Return endpoints for the weakly connected components algorithm.
        """
        return WccCypherEndpoints(self._query_runner)

    def set_database(self, database: str) -> None:
        """
        Set the database which queries are run against.

        Parameters
        -------
        database: str
            The name of the database to run queries against.
        """
        self._query_runner.set_database(database)

    def set_bookmarks(self, bookmarks: Any) -> None:
        """
        Set Neo4j bookmarks to require a certain state before the next query gets executed

        Parameters
        ----------
        bookmarks: Bookmark(s)
            The Neo4j bookmarks defining the required state
        """
        self._query_runner.set_bookmarks(bookmarks)

    def set_show_progress(self, show_progress: bool) -> None:
        """
        Set whether to show progress for running procedures.

        Parameters
        ----------
        show_progress: bool
            Whether to show progress for procedures.
        """
        self._query_runner.set_show_progress(show_progress)

    def database(self) -> str | None:
        """
        Get the database which queries are run against.

        Returns:
            The name of the database.
        """
        return self._query_runner.database()

    def bookmarks(self) -> Any | None:
        """
        Get the Neo4j bookmarks defining the currently required states for queries to execute

        Returns
        -------
        The (possibly None) Neo4j bookmarks defining the currently required state
        """
        return self._query_runner.bookmarks()

    def last_bookmarks(self) -> Any | None:
        """
        Get the Neo4j bookmarks defining the state following the most recently called query

        Returns
        -------
        The (possibly None) Neo4j bookmarks defining the state following the most recently called query
        """
        return self._query_runner.last_bookmarks()

    def run_cypher(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        database: str | None = None,
        retryable: bool = False,
        mode: QueryMode = QueryMode.WRITE,
    ) -> DataFrame:
        """
        Run a Cypher query

        Parameters
        ----------
        query: str
            the Cypher query
        params: dict[str, Any]
            parameters to the query
        database: str
            the database on which to run the query
        retryable: bool
            whether the query can be automatically retried. Make sure the query is idempotent if set to True.
        mode: QueryMode
            the query mode to use (READ or WRITE). Set based on the operation performed in the query.

        Returns:
            The query result as a DataFrame
        """
        query_type = QueryType.USER_DIRECTED

        if retryable:
            return self._query_runner.run_retryable_cypher(
                query, query_type, params, database, custom_error=False, mode=mode
            )
        else:
            return self._query_runner.run_cypher(query, query_type, params, database, custom_error=False, mode=mode)

    def driver_config(self) -> dict[str, Any]:
        """
        Get the configuration used to create the underlying driver used to make queries to Neo4j.

        Returns:
            The configuration as a dictionary.
        """
        return self._query_runner.driver_config()

    @staticmethod
    def _validate_endpoint(endpoint: str | Driver | QueryRunner) -> None:
        if isinstance(endpoint, str):
            protocol = endpoint.split(":")[0]
            if protocol != Neo4jQueryRunner._AURA_DS_PROTOCOL:
                raise ValueError(
                    (
                        f"AuraDS requires using the '{Neo4jQueryRunner._AURA_DS_PROTOCOL}'"
                        f" protocol ('{protocol}' was provided)",
                    )
                )

    def close(self) -> None:
        """
        Close the GraphDataScience object and release any resources held by it.

        If the GraphDataScience object was instantiated with a Neo4j Driver, the driver will not be closed as we cannot assume sole ownership of it.
        """
        self._query_runner.close()

    def __enter__(self) -> GraphDataScience:
        return self

    def __exit__(
        self,
        exception_type: Type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def server_version(self) -> ServerVersion:
        return self._server_version
