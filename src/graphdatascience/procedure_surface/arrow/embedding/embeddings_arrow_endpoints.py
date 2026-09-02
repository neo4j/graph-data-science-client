from __future__ import annotations

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.embedding.embeddings_endpoints import EmbeddingsEndpoints
from graphdatascience.procedure_surface.arrow.embedding.encode_arrow_endpoints import EncodeArrowEndpoints
from graphdatascience.procedure_surface.arrow.embedding.predict_arrow_endpoints import PredictArrowEndpoints
from graphdatascience.procedure_surface.arrow.embedding.train_arrow_endpoints import TrainArrowEndpoints
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol


class EmbeddingsArrowEndpoints(EmbeddingsEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None = None,
        show_progress: bool = True,
    ) -> None:
        self._arrow_client = arrow_client
        self._write_protocol = write_protocol
        self._show_progress = show_progress

    @property
    def train(self) -> TrainArrowEndpoints:
        return TrainArrowEndpoints(self._arrow_client, self._write_protocol, show_progress=self._show_progress)

    @property
    def predict(self) -> PredictArrowEndpoints:
        return PredictArrowEndpoints(self._arrow_client, self._write_protocol, show_progress=self._show_progress)

    @property
    def encode(self) -> EncodeArrowEndpoints:
        return EncodeArrowEndpoints(self._arrow_client, self._write_protocol, show_progress=self._show_progress)
