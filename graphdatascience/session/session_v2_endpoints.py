from typing import Optional

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.arrow.articlerank_arrow_endpoints import ArticleRankArrowEndpoints
from graphdatascience.procedure_surface.arrow.articulationpoints_arrow_endpoints import ArticulationPointsArrowEndpoints
from graphdatascience.procedure_surface.arrow.betweenness_arrow_endpoints import BetweennessArrowEndpoints
from graphdatascience.procedure_surface.arrow.catalog_arrow_endpoints import CatalogArrowEndpoints
from graphdatascience.procedure_surface.arrow.celf_arrow_endpoints import CelfArrowEndpoints
from graphdatascience.procedure_surface.arrow.closeness_arrow_endpoints import ClosenessArrowEndpoints
from graphdatascience.procedure_surface.arrow.closeness_harmonic_arrow_endpoints import ClosenessHarmonicArrowEndpoints
from graphdatascience.procedure_surface.arrow.degree_arrow_endpoints import DegreeArrowEndpoints
from graphdatascience.procedure_surface.arrow.eigenvector_arrow_endpoints import EigenvectorArrowEndpoints
from graphdatascience.procedure_surface.arrow.fastrp_arrow_endpoints import FastRPArrowEndpoints
from graphdatascience.procedure_surface.arrow.graphsage_predict_arrow_endpoints import GraphSagePredictArrowEndpoints
from graphdatascience.procedure_surface.arrow.graphsage_train_arrow_endpoints import GraphSageTrainArrowEndpoints
from graphdatascience.procedure_surface.arrow.hashgnn_arrow_endpoints import HashGNNArrowEndpoints
from graphdatascience.procedure_surface.arrow.k1coloring_arrow_endpoints import K1ColoringArrowEndpoints
from graphdatascience.procedure_surface.arrow.kcore_arrow_endpoints import KCoreArrowEndpoints
from graphdatascience.procedure_surface.arrow.louvain_arrow_endpoints import LouvainArrowEndpoints
from graphdatascience.procedure_surface.arrow.node2vec_arrow_endpoints import Node2VecArrowEndpoints
from graphdatascience.procedure_surface.arrow.pagerank_arrow_endpoints import PageRankArrowEndpoints
from graphdatascience.procedure_surface.arrow.scc_arrow_endpoints import SccArrowEndpoints
from graphdatascience.procedure_surface.arrow.wcc_arrow_endpoints import WccArrowEndpoints
from graphdatascience.query_runner.query_runner import QueryRunner


class SessionV2Endpoints:
    def __init__(self, arrow_client: AuthenticatedArrowClient, db_client: Optional[QueryRunner] = None):
        self._arrow_client = arrow_client
        self._db_client = db_client

        self._write_back_client = RemoteWriteBackClient(arrow_client, db_client) if db_client is not None else None

    @property
    def graph(self) -> CatalogArrowEndpoints:
        return CatalogArrowEndpoints(self._arrow_client, self._db_client)

    ## Algorithms

    @property
    def article_rank(self) -> ArticleRankArrowEndpoints:
        return ArticleRankArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def articulation_points(self) -> ArticulationPointsArrowEndpoints:
        return ArticulationPointsArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def betweenness_centrality(self) -> BetweennessArrowEndpoints:
        return BetweennessArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def closeness_centrality(self) -> ClosenessArrowEndpoints:
        return ClosenessArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def degree_centrality(self) -> DegreeArrowEndpoints:
        return DegreeArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def eigenvector_centrality(self) -> EigenvectorArrowEndpoints:
        return EigenvectorArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def fast_rp(self) -> FastRPArrowEndpoints:
        return FastRPArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def graphsage_predict(self) -> GraphSagePredictArrowEndpoints:
        return GraphSagePredictArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def graphsage_train(self) -> GraphSageTrainArrowEndpoints:
        return GraphSageTrainArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def harmonic_centrality(self) -> ClosenessHarmonicArrowEndpoints:
        return ClosenessHarmonicArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def hash_gnn(self) -> HashGNNArrowEndpoints:
        return HashGNNArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def influence_maximization_celf(self) -> CelfArrowEndpoints:
        return CelfArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def k1_coloring(self) -> K1ColoringArrowEndpoints:
        return K1ColoringArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def k_core_decomposition(self) -> KCoreArrowEndpoints:
        return KCoreArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def louvain(self) -> LouvainArrowEndpoints:
        return LouvainArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def node2vec(self) -> Node2VecArrowEndpoints:
        return Node2VecArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def page_rank(self) -> PageRankArrowEndpoints:
        return PageRankArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def scc(self) -> SccArrowEndpoints:
        return SccArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def wcc(self) -> WccArrowEndpoints:
        return WccArrowEndpoints(self._arrow_client, self._write_back_client)
