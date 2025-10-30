from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
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
from graphdatascience.procedure_surface.api.node_embedding.fastrp_endpoints import FastRPEndpoints
from graphdatascience.procedure_surface.api.node_embedding.graphsage_endpoints import GraphSageEndpoints
from graphdatascience.procedure_surface.api.node_embedding.hashgnn_endpoints import HashGNNEndpoints
from graphdatascience.procedure_surface.api.node_embedding.node2vec_endpoints import Node2VecEndpoints
from graphdatascience.procedure_surface.api.pathfinding.all_shortest_path_endpoints import AllShortestPathEndpoints
from graphdatascience.procedure_surface.api.pathfinding.k_spanning_tree_endpoints import KSpanningTreeEndpoints
from graphdatascience.procedure_surface.api.pathfinding.prize_steiner_tree_endpoints import PrizeSteinerTreeEndpoints
from graphdatascience.procedure_surface.api.pathfinding.shortest_path_endpoints import ShortestPathEndpoints
from graphdatascience.procedure_surface.api.pathfinding.spanning_tree_endpoints import SpanningTreeEndpoints
from graphdatascience.procedure_surface.api.pathfinding.steiner_tree_endpoints import SteinerTreeEndpoints
from graphdatascience.procedure_surface.api.similarity.knn_endpoints import KnnEndpoints
from graphdatascience.procedure_surface.api.similarity.node_similarity_endpoints import NodeSimilarityEndpoints
from graphdatascience.procedure_surface.arrow.catalog_arrow_endpoints import CatalogArrowEndpoints
from graphdatascience.procedure_surface.arrow.centrality.articlerank_arrow_endpoints import ArticleRankArrowEndpoints
from graphdatascience.procedure_surface.arrow.centrality.articulationpoints_arrow_endpoints import (
    ArticulationPointsArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.centrality.betweenness_arrow_endpoints import BetweennessArrowEndpoints
from graphdatascience.procedure_surface.arrow.centrality.celf_arrow_endpoints import CelfArrowEndpoints
from graphdatascience.procedure_surface.arrow.centrality.closeness_arrow_endpoints import ClosenessArrowEndpoints
from graphdatascience.procedure_surface.arrow.centrality.closeness_harmonic_arrow_endpoints import (
    ClosenessHarmonicArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.centrality.degree_arrow_endpoints import DegreeArrowEndpoints
from graphdatascience.procedure_surface.arrow.centrality.eigenvector_arrow_endpoints import EigenvectorArrowEndpoints
from graphdatascience.procedure_surface.arrow.centrality.pagerank_arrow_endpoints import PageRankArrowEndpoints
from graphdatascience.procedure_surface.arrow.community.clique_counting_arrow_endpoints import (
    CliqueCountingArrowEndpoints,
)
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
from graphdatascience.procedure_surface.arrow.community.modularity_optimization_arrow_endpoints import (
    ModularityOptimizationArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.community.scc_arrow_endpoints import SccArrowEndpoints
from graphdatascience.procedure_surface.arrow.community.sllpa_arrow_endpoints import SllpaArrowEndpoints
from graphdatascience.procedure_surface.arrow.community.triangle_count_arrow_endpoints import (
    TriangleCountArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.community.wcc_arrow_endpoints import WccArrowEndpoints
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
from graphdatascience.procedure_surface.arrow.pathfinding.k_spanning_tree_arrow_endpoints import (
    KSpanningTreeArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pathfinding.prize_steiner_tree_arrow_endpoints import (
    PrizeSteinerTreeArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pathfinding.shortest_path_arrow_endpoints import (
    ShortestPathArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pathfinding.spanning_tree_arrow_endpoints import (
    SpanningTreeArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pathfinding.steiner_tree_arrow_endpoints import (
    SteinerTreeArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.similarity.knn_arrow_endpoints import KnnArrowEndpoints
from graphdatascience.procedure_surface.arrow.similarity.node_similarity_arrow_endpoints import (
    NodeSimilarityArrowEndpoints,
)
from graphdatascience.query_runner.query_runner import QueryRunner


class SessionV2Endpoints:
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        db_client: QueryRunner | None = None,
        show_progress: bool = False,
    ):
        self._arrow_client = arrow_client
        self._db_client = db_client
        self._show_progress = show_progress

        self._write_back_client = RemoteWriteBackClient(arrow_client, db_client) if db_client is not None else None

    def set_show_progress(self, show_progress: bool) -> None:
        self._show_progress = show_progress

    @property
    def graph(self) -> CatalogArrowEndpoints:
        return CatalogArrowEndpoints(self._arrow_client, self._db_client, show_progress=self._show_progress)

    ## Algorithms

    @property
    def all_shortest_path(self) -> AllShortestPathEndpoints:
        return AllShortestPathArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def article_rank(self) -> ArticleRankEndpoints:
        return ArticleRankArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def articulation_points(self) -> ArticulationPointsEndpoints:
        return ArticulationPointsArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def betweenness_centrality(self) -> BetweennessEndpoints:
        return BetweennessArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def clique_counting(self) -> CliqueCountingEndpoints:
        return CliqueCountingArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def closeness_centrality(self) -> ClosenessEndpoints:
        return ClosenessArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def degree_centrality(self) -> DegreeEndpoints:
        return DegreeArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def eigenvector_centrality(self) -> EigenvectorEndpoints:
        return EigenvectorArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def fast_rp(self) -> FastRPEndpoints:
        return FastRPArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def graph_sage(self) -> GraphSageEndpoints:
        return GraphSageEndpoints(
            train_endpoints=GraphSageTrainArrowEndpoints(
                self._arrow_client, self._write_back_client, show_progress=self._show_progress
            ),
            predict_endpoints=GraphSagePredictArrowEndpoints(
                self._arrow_client, self._write_back_client, show_progress=self._show_progress
            ),
        )

    @property
    def harmonic_centrality(self) -> ClosenessHarmonicEndpoints:
        return ClosenessHarmonicArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def hash_gnn(self) -> HashGNNEndpoints:
        return HashGNNArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def hdbscan(self) -> HdbscanEndpoints:
        return HdbscanArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def influence_maximization_celf(self) -> CelfEndpoints:
        return CelfArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def k1_coloring(self) -> K1ColoringEndpoints:
        return K1ColoringArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def k_core_decomposition(self) -> KCoreEndpoints:
        return KCoreArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def kmeans(self) -> KMeansEndpoints:
        return KMeansArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def knn(self) -> KnnEndpoints:
        return KnnArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def k_spanning_tree(self) -> KSpanningTreeEndpoints:
        return KSpanningTreeArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def label_propagation(self) -> LabelPropagationEndpoints:
        return LabelPropagationArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def leiden(self) -> LeidenEndpoints:
        return LeidenArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def local_clustering_coefficient(self) -> LocalClusteringCoefficientEndpoints:
        return LocalClusteringCoefficientArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def louvain(self) -> LouvainEndpoints:
        return LouvainArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def max_k_cut(self) -> MaxKCutEndpoints:
        return MaxKCutArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def modularity_optimization(self) -> ModularityOptimizationEndpoints:
        return ModularityOptimizationArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def node2vec(self) -> Node2VecEndpoints:
        return Node2VecArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def node_similarity(self) -> NodeSimilarityEndpoints:
        return NodeSimilarityArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def page_rank(self) -> PageRankEndpoints:
        return PageRankArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def prize_steiner_tree(self) -> PrizeSteinerTreeEndpoints:
        return PrizeSteinerTreeArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def scc(self) -> SccEndpoints:
        return SccArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def shortest_path(self) -> ShortestPathEndpoints:
        return ShortestPathArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def spanning_tree(self) -> SpanningTreeEndpoints:
        return SpanningTreeArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def steiner_tree(self) -> SteinerTreeEndpoints:
        return SteinerTreeArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def sllpa(self) -> SllpaEndpoints:
        return SllpaArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def triangle_count(self) -> TriangleCountEndpoints:
        return TriangleCountArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def wcc(self) -> WccEndpoints:
        return WccArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)
