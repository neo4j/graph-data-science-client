from __future__ import annotations

from typing import Any, Tuple

from pandas import DataFrame

from graphdatascience.arrow_client.arrow_authentication import ArrowAuthentication
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.gds_arrow_client import GdsArrowClient
from graphdatascience.error.standalone_session_error import NotAvailableInStandaloneSessions
from graphdatascience.procedure_surface.api import ConfigEndpoints
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
from graphdatascience.procedure_surface.api.list_progress_endpoint import ListProgressEndpoint
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
from graphdatascience.procedure_surface.api.util_endpoints import UtilEndpoints
from graphdatascience.procedure_surface.arrow.catalog.catalog_arrow_endpoints import CatalogArrowEndpoints
from graphdatascience.procedure_surface.arrow.catalog.scale_properties_arrow_endpoints import (
    ScalePropertiesArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.centrality.articlerank_arrow_endpoints import ArticleRankArrowEndpoints
from graphdatascience.procedure_surface.arrow.centrality.articulationpoints_arrow_endpoints import (
    ArticulationPointsArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.centrality.betweenness_arrow_endpoints import BetweennessArrowEndpoints
from graphdatascience.procedure_surface.arrow.centrality.bridges_arrow_endpoints import BridgesArrowEndpoints
from graphdatascience.procedure_surface.arrow.centrality.celf_arrow_endpoints import CelfArrowEndpoints
from graphdatascience.procedure_surface.arrow.centrality.closeness_arrow_endpoints import ClosenessArrowEndpoints
from graphdatascience.procedure_surface.arrow.centrality.closeness_harmonic_arrow_endpoints import (
    ClosenessHarmonicArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.centrality.degree_arrow_endpoints import DegreeArrowEndpoints
from graphdatascience.procedure_surface.arrow.centrality.eigenvector_arrow_endpoints import EigenvectorArrowEndpoints
from graphdatascience.procedure_surface.arrow.centrality.pagerank_arrow_endpoints import PageRankArrowEndpoints
from graphdatascience.procedure_surface.arrow.collapse_path_arrow_endpoints import CollapsePathArrowEndpoints
from graphdatascience.procedure_surface.arrow.community.clique_counting_arrow_endpoints import (
    CliqueCountingArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.community.conductance_arrow_endpoints import ConductanceArrowEndpoints
from graphdatascience.procedure_surface.arrow.community.hdbscan_arrow_endpoints import HdbscanArrowEndpoints
from graphdatascience.procedure_surface.arrow.community.k1coloring_arrow_endpoints import K1ColoringArrowEndpoints
from graphdatascience.procedure_surface.arrow.community.kcore_arrow_endpoints import KCoreArrowEndpoints
from graphdatascience.procedure_surface.arrow.community.kmeans_arrow_endpoints import KMeansArrowEndpoints
from graphdatascience.procedure_surface.arrow.community.labelpropagation_arrow_endpoints import (
    LabelPropagationArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.community.leiden_arrow_endpoints import LeidenArrowEndpoints
from graphdatascience.procedure_surface.arrow.community.local_clustering_coefficient_arrow_endpoints import (
    LocalClusteringCoefficientArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.community.louvain_arrow_endpoints import LouvainArrowEndpoints
from graphdatascience.procedure_surface.arrow.community.maxkcut_arrow_endpoints import MaxKCutArrowEndpoints
from graphdatascience.procedure_surface.arrow.community.modularity_arrow_endpoints import ModularityArrowEndpoints
from graphdatascience.procedure_surface.arrow.community.modularity_optimization_arrow_endpoints import (
    ModularityOptimizationArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.community.scc_arrow_endpoints import SccArrowEndpoints
from graphdatascience.procedure_surface.arrow.community.sllpa_arrow_endpoints import SllpaArrowEndpoints
from graphdatascience.procedure_surface.arrow.community.triangle_count_arrow_endpoints import (
    TriangleCountArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.community.triangles_arrow_endpoints import TrianglesArrowEndpoints
from graphdatascience.procedure_surface.arrow.community.wcc_arrow_endpoints import WccArrowEndpoints
from graphdatascience.procedure_surface.arrow.config_arrow_endpoints import ConfigArrowEndpoints
from graphdatascience.procedure_surface.arrow.jobs_arrow_endpoints import JobsArrowEndpoints
from graphdatascience.procedure_surface.arrow.list_progress_arrow_endpoint import ListProgressArrowEndpoint
from graphdatascience.procedure_surface.arrow.model.model_catalog_arrow_endpoints import (
    ModelCatalogArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.node_embedding.fastrp_arrow_endpoints import FastRPArrowEndpoints
from graphdatascience.procedure_surface.arrow.node_embedding.graphsage_predict_arrow_endpoints import (
    GraphSagePredictArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.node_embedding.graphsage_train_arrow_endpoints import (
    GraphSageTrainArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.node_embedding.hashgnn_arrow_endpoints import HashGNNArrowEndpoints
from graphdatascience.procedure_surface.arrow.node_embedding.node2vec_arrow_endpoints import Node2VecArrowEndpoints
from graphdatascience.procedure_surface.arrow.pathfinding.all_shortest_path_arrow_endpoints import (
    AllShortestPathArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pathfinding.bfs_arrow_endpoints import BFSArrowEndpoints
from graphdatascience.procedure_surface.arrow.pathfinding.dag_arrow_endpoints import DagArrowEndpoints
from graphdatascience.procedure_surface.arrow.pathfinding.dfs_arrow_endpoints import DFSArrowEndpoints
from graphdatascience.procedure_surface.arrow.pathfinding.k_spanning_tree_arrow_endpoints import (
    KSpanningTreeArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pathfinding.max_flow_arrow_endpoints import MaxFlowArrowEndpoints
from graphdatascience.procedure_surface.arrow.pathfinding.prize_steiner_tree_arrow_endpoints import (
    PrizeSteinerTreeArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pathfinding.random_walk_arrow_endpoints import (
    RandomWalkArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pathfinding.shortest_path_arrow_endpoints import (
    ShortestPathArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pathfinding.single_source_bellman_ford_arrow_endpoints import (
    BellmanFordArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pathfinding.spanning_tree_arrow_endpoints import (
    SpanningTreeArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pathfinding.steiner_tree_arrow_endpoints import (
    SteinerTreeArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pipeline.pipeline_arrow_endpoints import (
    PipelineArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.similarity.knn_arrow_endpoints import KnnArrowEndpoints
from graphdatascience.procedure_surface.arrow.similarity.node_similarity_arrow_endpoints import (
    NodeSimilarityArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.util_arrow_endpoints import UtilArrowEndpoints
from graphdatascience.query_runner import QueryRunner
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.query_runner.query_type import QueryType
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol
from graphdatascience.session.session_lifecycle_manager import LifecycleManager


class AuraGraphDataScience:
    """
    Primary API class for interacting with Neo4j database + Graph Data Science Session.
    Always bind this object to a variable called `gds`.
    """

    @classmethod
    def create(
        cls,
        session_connection_info: str | Tuple[str, int],
        arrow_authentication: ArrowAuthentication | None,
        db_endpoint: Neo4jQueryRunner | DbmsConnectionInfo | None,
        session_lifecycle_manager: LifecycleManager,
        encrypted: bool = True,
        arrow_client_options: dict[str, Any] | None = None,
        bookmarks: Any | None = None,
        show_progress: bool = True,
    ) -> AuraGraphDataScience:
        authenticated_arrow_client = AuthenticatedArrowClient(
            session_connection_info,
            auth=arrow_authentication,
            encrypted=encrypted,
            arrow_client_options=arrow_client_options,
        )

        db_query_runner: Neo4jQueryRunner | None = None
        if db_endpoint is not None:
            if isinstance(db_endpoint, Neo4jQueryRunner):
                db_query_runner = db_endpoint
            else:
                db_query_runner = Neo4jQueryRunner.create_for_db(
                    db_endpoint.get_uri(),
                    db_endpoint.get_auth(),
                    aura_ds=True,
                    show_progress=False,
                    database=db_endpoint.database,
                )
            db_query_runner.set_bookmarks(bookmarks)

        return cls(
            authenticated_arrow_client,
            db_query_runner,
            session_lifecycle_manager=session_lifecycle_manager,
            show_progress=show_progress,
        )

    def __init__(
        self,
        authenticated_arrow_client: AuthenticatedArrowClient,
        db_query_runner: QueryRunner | None,
        session_lifecycle_manager: LifecycleManager,
        show_progress: bool = True,
    ):
        self._authenticated_arrow_client = authenticated_arrow_client
        self._db_query_runner = db_query_runner
        self._write_protocol: WriteProtocol | None = None
        if db_query_runner:
            self._write_protocol = WriteProtocol.select(authenticated_arrow_client, db_query_runner)
        self._session_lifecycle_manager = session_lifecycle_manager
        self._show_progress = show_progress

    @property
    def graph(self) -> CatalogArrowEndpoints:
        """
        Return graph-related endpoints for graph management.
        """
        return CatalogArrowEndpoints(
            self._authenticated_arrow_client, self._db_query_runner, show_progress=self._show_progress
        )

    @property
    def model(self) -> ModelCatalogEndpoints:
        """
        Return model-related endpoints for model management.
        """
        return ModelCatalogArrowEndpoints(self._authenticated_arrow_client)

    @property
    def config(self) -> ConfigEndpoints:
        """
        Return configuration-related endpoints.
        """
        return ConfigArrowEndpoints(self._authenticated_arrow_client)

    @property
    def util(self) -> UtilEndpoints:
        """
        Return utility endpoints.
        """
        return UtilArrowEndpoints(self._db_query_runner)

    @property
    def list_progress(self) -> ListProgressEndpoint:
        """
        Return system-related endpoints.
        """
        return ListProgressArrowEndpoint(self._authenticated_arrow_client)

    @property
    def jobs(self) -> JobsArrowEndpoints:
        """
        Return endpoints for inspecting and controlling jobs (get/list).
        """
        return JobsArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def collapse_path(self) -> CollapsePathEndpoints:
        """
        Return endpoints for collapsing relationship paths.
        """
        return CollapsePathArrowEndpoints(self._authenticated_arrow_client, show_progress=self._show_progress)

    ## Algorithms

    @property
    def all_shortest_paths(self) -> AllShortestPathEndpoints:
        """
        Return endpoints for the all shortest paths algorithm.
        """
        return AllShortestPathArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def article_rank(self) -> ArticleRankEndpoints:
        """
        Return endpoints for the article rank algorithm.
        """
        return ArticleRankArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def bfs(self) -> BFSEndpoints:
        """
        Return endpoints for the Breadth First Search (BFS) algorithm.
        """
        return BFSArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def dfs(self) -> DFSEndpoints:
        """
        Return endpoints for the Depth First Search (DFS) algorithm.
        """
        return DFSArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def articulation_points(self) -> ArticulationPointsEndpoints:
        """
        Return endpoints for the articulation points algorithm.
        """
        return ArticulationPointsArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def betweenness_centrality(self) -> BetweennessEndpoints:
        """
        Return endpoints for the betweenness centrality algorithm.
        """
        return BetweennessArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def bridges(self) -> BridgesEndpoints:
        """
        Return endpoints for the bridges algorithm.
        """
        return BridgesArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def bellman_ford(self) -> SingleSourceBellmanFordEndpoints:
        """
        Return endpoints for the single source Bellman-Ford algorithm.
        """
        return BellmanFordArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def clique_counting(self) -> CliqueCountingEndpoints:
        """
        Return endpoints for the clique counting algorithm.
        """
        return CliqueCountingArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def conductance(self) -> ConductanceEndpoints:
        """
        Return endpoints for the conductance algorithm.
        """
        return ConductanceArrowEndpoints(self._authenticated_arrow_client, show_progress=self._show_progress)

    @property
    def modularity(self) -> ModularityEndpoints:
        """
        Return endpoints for the modularity algorithm.
        """
        return ModularityArrowEndpoints(self._authenticated_arrow_client, show_progress=self._show_progress)

    @property
    def closeness_centrality(self) -> ClosenessEndpoints:
        """
        Return endpoints for the closeness centrality algorithm.
        """
        return ClosenessArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def dag(self) -> DagEndpoints:
        """
        Return endpoints for Directed Acyclic Graph (DAG) algorithms.
        """
        return DagArrowEndpoints(self._authenticated_arrow_client, show_progress=self._show_progress)

    @property
    def degree_centrality(self) -> DegreeEndpoints:
        """
        Return endpoints for the degree centrality algorithm.
        """
        return DegreeArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def eigenvector_centrality(self) -> EigenvectorEndpoints:
        """
        Return endpoints for the eigenvector centrality algorithm.
        """
        return EigenvectorArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def fast_rp(self) -> FastRPEndpoints:
        """
        Return endpoints for the fast RP algorithm.
        """
        return FastRPArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def graph_sage(self) -> GraphSageEndpoints:
        """
        Return endpoints for the GraphSage algorithm.
        """
        return GraphSageEndpoints(
            train_endpoints=GraphSageTrainArrowEndpoints(
                self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
            ),
            predict_endpoints=GraphSagePredictArrowEndpoints(
                self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
            ),
        )

    @property
    def harmonic_centrality(self) -> ClosenessHarmonicEndpoints:
        """
        Return endpoints for the harmonic centrality algorithm.
        """
        return ClosenessHarmonicArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def hash_gnn(self) -> HashGNNEndpoints:
        """
        Return endpoints for the HashGNN algorithm.
        """
        return HashGNNArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def hdbscan(self) -> HdbscanEndpoints:
        """
        Return endpoints for the HDBSCAN algorithm.
        """
        return HdbscanArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def influence_maximization_celf(self) -> CelfEndpoints:
        """
        Return endpoints for the influence maximization CELF algorithm.
        """
        return CelfArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def k1_coloring(self) -> K1ColoringEndpoints:
        """
        Return endpoints for the K1 coloring algorithm.
        """
        return K1ColoringArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def k_core_decomposition(self) -> KCoreEndpoints:
        """
        Return endpoints for the K-core decomposition algorithm.
        """
        return KCoreArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def kmeans(self) -> KMeansEndpoints:
        """
        Return endpoints for the K-means algorithm.
        """
        return KMeansArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def knn(self) -> KnnEndpoints:
        """
        Return endpoints for the K-nearest neighbors algorithm.
        """
        return KnnArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def k_spanning_tree(self) -> KSpanningTreeEndpoints:
        """
        Return endpoints for the K-spanning tree algorithm.
        """
        return KSpanningTreeArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def label_propagation(self) -> LabelPropagationEndpoints:
        """
        Return endpoints for the label propagation algorithm.
        """
        return LabelPropagationArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def leiden(self) -> LeidenEndpoints:
        """
        Return endpoints for the Leiden algorithm.
        """
        return LeidenArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def max_flow(self) -> MaxFlowEndpoints:
        """
        Return endpoints for the Max Flow algorithm.
        """
        return MaxFlowArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def local_clustering_coefficient(self) -> LocalClusteringCoefficientEndpoints:
        """
        Return endpoints for the local clustering coefficient algorithm.
        """
        return LocalClusteringCoefficientArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def louvain(self) -> LouvainEndpoints:
        """
        Return endpoints for the Louvain algorithm.
        """
        return LouvainArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def max_k_cut(self) -> MaxKCutEndpoints:
        """
        Return endpoints for the Max K-cut algorithm.
        """
        return MaxKCutArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def modularity_optimization(self) -> ModularityOptimizationEndpoints:
        """
        Return endpoints for the modularity optimization algorithm.
        """
        return ModularityOptimizationArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def node2vec(self) -> Node2VecEndpoints:
        """
        Return endpoints for the Node2Vec algorithm.
        """
        return Node2VecArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def node_similarity(self) -> NodeSimilarityEndpoints:
        """
        Return endpoints for the node similarity algorithm.
        """
        return NodeSimilarityArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

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
        return PageRankArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def prize_steiner_tree(self) -> PrizeSteinerTreeEndpoints:
        """
        Return endpoints for the prize-collecting Steiner tree algorithm.
        """
        return PrizeSteinerTreeArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def random_walk(self) -> RandomWalkEndpoints:
        """
        Return endpoints for the Random Walk algorithm.
        """
        return RandomWalkArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def scc(self) -> SccEndpoints:
        """
        Return endpoints for the strongly connected components algorithm.
        """
        return SccArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def scale_properties(self) -> ScalePropertiesEndpoints:
        """
        Return endpoints for scaling node properties.
        """
        return ScalePropertiesArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def shortest_path(self) -> ShortestPathEndpoints:
        """
        Return endpoints for the shortest path algorithm.
        """
        return ShortestPathArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def spanning_tree(self) -> SpanningTreeEndpoints:
        """
        Return endpoints for the spanning tree algorithm.
        """
        return SpanningTreeArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def steiner_tree(self) -> SteinerTreeEndpoints:
        """
        Return endpoints for the Steiner tree algorithm.
        """
        return SteinerTreeArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def sllpa(self) -> SllpaEndpoints:
        """
        Return endpoints for the speaker-listener label propagation algorithm.
        """
        return SllpaArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def triangle_count(self) -> TriangleCountEndpoints:
        """
        Return endpoints for the triangle count algorithm.
        """
        return TriangleCountArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def pipeline(self) -> PipelineEndpoints:
        """
        Return endpoints for pipeline procedures.
        """
        return PipelineArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    @property
    def triangles(self) -> TrianglesEndpoints:
        """
        Return endpoint for the triangles algorithm.
        """
        return TrianglesArrowEndpoints(self._authenticated_arrow_client, show_progress=self._show_progress)

    @property
    def wcc(self) -> WccEndpoints:
        """
        Return endpoints for the weakly connected components algorithm.
        """
        return WccArrowEndpoints(
            self._authenticated_arrow_client, self._write_protocol, show_progress=self._show_progress
        )

    def verify_connectivity(self) -> None:
        """
        Verifies that Aura Graph Analytics Session is ready and the connection to the Aura Graph Analytics Session can be established.
        Also verifies the connection to the Neo4j database, if specified.

        :raises Exception: if the session is not running anymore, the session cannot be reached or the database driver cannot connect to the remote.
        """
        self._session_lifecycle_manager.verify_health()
        self._verify_session_connectivity()
        self._verify_db_connectivity()

    def run_cypher(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        database: str | None = None,
        retryable: bool = False,
        mode: QueryMode = QueryMode.WRITE,
    ) -> DataFrame:
        """
        Run a Cypher query against the Neo4j database.

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

        Returns
        -------
        DataFrame
            The query result as a DataFrame
        """
        if not self._db_query_runner:
            raise NotAvailableInStandaloneSessions("Running Cypher queries")

        if retryable:
            return self._db_query_runner.run_retryable_cypher(
                query, QueryType.USER_DIRECTED, params, database, custom_error=False, mode=mode
            )
        else:
            return self._db_query_runner.run_cypher(
                query, QueryType.USER_DIRECTED, params, database, custom_error=False, mode=mode
            )

    def arrow_client(self) -> GdsArrowClient:
        """
        Returns a GdsArrowClient that is authenticated to communicate with the Aura Graph Analytics Session.
        This client can be used to get direct access to the specific session's Arrow Flight server.

        Returns:
            A GdsArrowClient
        -------

        """
        return GdsArrowClient(self._authenticated_arrow_client)

    def set_database(self, database: str) -> None:
        """
        Set the database which cypher queries are run against.

        Parameters
        -------
        database: str
            The name of the database to run queries against.
        """
        if not self._db_query_runner:
            raise NotAvailableInStandaloneSessions("Setting the database")
        self._db_query_runner.set_database(database)

    def set_bookmarks(self, bookmarks: Any) -> None:
        """
        Set Neo4j bookmarks to require a certain state before the next query gets executed

        Parameters
        ----------
        bookmarks: Bookmark(s)
            The Neo4j bookmarks defining the required state
        """
        if not self._db_query_runner:
            raise NotAvailableInStandaloneSessions("Setting bookmarks")
        self._db_query_runner.set_bookmarks(bookmarks)

    def set_show_progress(self, show_progress: bool) -> None:
        """
        Set whether to show progress for running procedures.

        Parameters
        ----------
        show_progress: bool
            Whether to show progress for procedures.
        """
        self._show_progress = show_progress
        if self._db_query_runner:
            self._db_query_runner.set_show_progress(show_progress)

    def database(self) -> str | None:
        """
        Get the database which cypher queries are run against.

        Returns:
            The name of the database.
        """
        if not self._db_query_runner:
            raise NotAvailableInStandaloneSessions("Getting the database")
        return self._db_query_runner.database()

    def bookmarks(self) -> Any | None:
        """
        Get the Neo4j bookmarks defining the currently required states for cypher queries to execute

        Returns
        -------
        The (possibly None) Neo4j bookmarks defining the currently required state
        """
        if not self._db_query_runner:
            raise NotAvailableInStandaloneSessions("Getting bookmarks")
        return self._db_query_runner.bookmarks()

    def last_bookmarks(self) -> Any | None:
        """
        Get the Neo4j bookmarks defining the state following the most recently called query

        Returns
        -------
        The (possibly None) Neo4j bookmarks defining the state following the most recently called query
        """
        if not self._db_query_runner:
            raise NotAvailableInStandaloneSessions("Getting last bookmarks")
        return self._db_query_runner.last_bookmarks()

    def delete(self) -> bool:
        """
        Delete a GDS session.
        """
        self.close()
        return self._session_lifecycle_manager.delete()

    def close(self) -> None:
        """
        Close the GraphDataScience object and release any resources held by it.
        """
        self._authenticated_arrow_client.close()
        if self._db_query_runner:
            self._db_query_runner.close()

    def _verify_session_connectivity(self) -> None:
        """
        Verify connectivity to the Aura Graph Analytics session.

        :raises Exception: If the Aura Graph Analytics Session is unreachable
        """
        self._authenticated_arrow_client.request_token()

    def _verify_db_connectivity(self) -> None:
        """
        Verify connectivity to the Neo4j database.
        """
        if self._db_query_runner and isinstance(self._db_query_runner, Neo4jQueryRunner):
            self._db_query_runner.verify_connectivity()
