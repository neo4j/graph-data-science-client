from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.community.clique_counting_endpoints import CliqueCountingEndpoints
from graphdatascience.procedure_surface.api.community.kmeans_endpoints import KMeansEndpoints
from graphdatascience.procedure_surface.api.community.labelpropagation_endpoints import LabelPropagationEndpoints
from graphdatascience.procedure_surface.api.community.leiden_endpoints import LeidenEndpoints
from graphdatascience.procedure_surface.api.community.local_clustering_coefficient_endpoints import (
    LocalClusteringCoefficientEndpoints,
)
from graphdatascience.procedure_surface.api.community.maxkcut_endpoints import MaxKCutEndpoints
from graphdatascience.procedure_surface.api.community.modularity_optimization_endpoints import (
    ModularityOptimizationEndpoints,
)
from graphdatascience.procedure_surface.api.community.sllpa_endpoints import SllpaEndpoints
from graphdatascience.procedure_surface.api.community.triangle_count_endpoints import TriangleCountEndpoints
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
    def article_rank(self) -> ArticleRankArrowEndpoints:
        return ArticleRankArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def articulation_points(self) -> ArticulationPointsArrowEndpoints:
        return ArticulationPointsArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def betweenness_centrality(self) -> BetweennessArrowEndpoints:
        return BetweennessArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def clique_counting(self) -> CliqueCountingEndpoints:
        return CliqueCountingArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def closeness_centrality(self) -> ClosenessArrowEndpoints:
        return ClosenessArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def degree_centrality(self) -> DegreeArrowEndpoints:
        return DegreeArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def eigenvector_centrality(self) -> EigenvectorArrowEndpoints:
        return EigenvectorArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def fast_rp(self) -> FastRPArrowEndpoints:
        return FastRPArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def graphsage_predict(self) -> GraphSagePredictArrowEndpoints:
        return GraphSagePredictArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def graphsage_train(self) -> GraphSageTrainArrowEndpoints:
        return GraphSageTrainArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def harmonic_centrality(self) -> ClosenessHarmonicArrowEndpoints:
        return ClosenessHarmonicArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def hash_gnn(self) -> HashGNNArrowEndpoints:
        return HashGNNArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def influence_maximization_celf(self) -> CelfArrowEndpoints:
        return CelfArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def k1_coloring(self) -> K1ColoringArrowEndpoints:
        return K1ColoringArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def k_core_decomposition(self) -> KCoreArrowEndpoints:
        return KCoreArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def kmeans(self) -> KMeansEndpoints:
        return KMeansArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def label_propagation(self) -> LabelPropagationEndpoints:
        return LabelPropagationArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    def leiden(self) -> LeidenEndpoints:
        return LeidenArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def local_clustering_coefficient(self) -> LocalClusteringCoefficientEndpoints:
        return LocalClusteringCoefficientArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def louvain(self) -> LouvainArrowEndpoints:
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
    def node2vec(self) -> Node2VecArrowEndpoints:
        return Node2VecArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def page_rank(self) -> PageRankArrowEndpoints:
        return PageRankArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def scc(self) -> SccArrowEndpoints:
        return SccArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def sllpa(self) -> SllpaEndpoints:
        return SllpaArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)

    @property
    def triangle_count(self) -> TriangleCountEndpoints:
        return TriangleCountArrowEndpoints(
            self._arrow_client, self._write_back_client, show_progress=self._show_progress
        )

    @property
    def wcc(self) -> WccArrowEndpoints:
        return WccArrowEndpoints(self._arrow_client, self._write_back_client, show_progress=self._show_progress)
