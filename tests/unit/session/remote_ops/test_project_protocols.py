from unittest.mock import MagicMock

import pytest
from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import (
    AuthenticatedArrowClient,
    ConnectionInfo,
)
from graphdatascience.query_runner.termination_flag import TerminationFlagNoop
from graphdatascience.server_version.server_version import ServerVersion
from graphdatascience.session.dbms.protocol_version import ProtocolVersion
from graphdatascience.session.remote_ops.arrow_config import build_arrow_config
from graphdatascience.session.remote_ops.project_protocols import (
    ProjectProtocol,
    ProjectProtocolV3,
    ProjectProtocolV4,
)
from graphdatascience.session.remote_ops.status import Status
from tests.unit.conftest import CollectingQueryRunner


@pytest.fixture
def arrow_client() -> MagicMock:
    client = MagicMock(spec=AuthenticatedArrowClient)
    client.advertised_connection_info.return_value = ConnectionInfo(host="arrow.host", port=1234, encrypted=True)
    client.request_token.return_value = "some-token"
    return client


@pytest.fixture
def qr() -> CollectingQueryRunner:
    return CollectingQueryRunner(ServerVersion(2, 10, 0))


class TestSelect:
    def test_select_returns_v3(self, arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
        protocol = ProjectProtocol.select(ProtocolVersion.V3, arrow_client, qr, TerminationFlagNoop())
        assert isinstance(protocol, ProjectProtocolV3)

    def test_select_returns_v4(self, arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
        protocol = ProjectProtocol.select(ProtocolVersion.V4, arrow_client, qr, TerminationFlagNoop())
        assert isinstance(protocol, ProjectProtocolV4)

    def test_select_unsupported_version_raises(self, arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
        with pytest.raises(KeyError):
            ProjectProtocol.select(ProtocolVersion.V1, arrow_client, qr, TerminationFlagNoop())


def test_arrow_config_uses_advertised_connection_info(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    protocol = ProjectProtocolV4(arrow_client, qr, TerminationFlagNoop())

    config = build_arrow_config(protocol._arrow_client, 100)

    assert config == {
        "host": "arrow.host",
        "port": 1234,
        "token": "some-token",
        "encrypted": True,
        "batchSize": 100,
    }


class TestProjectProtocolV3:
    def test_start_cypher_projection_dispatches_expected_query_and_params(
        self, arrow_client: MagicMock, qr: CollectingQueryRunner
    ) -> None:
        qr.add__mock_result(
            "gds.arrow.project.v3",
            DataFrame([{"host": "member-host", "port": 7777}]),
        )

        protocol = ProjectProtocolV3(arrow_client, qr, TerminationFlagNoop())

        job_id, projection_runner = protocol.start_cypher_projection(
            graph_name="myGraph",
            query="MATCH (n) RETURN n",
            job_id="my-job",
            query_parameters={"foo": "bar"},
            concurrency=4,
            undirected_relationship_types=["REL"],
            inverse_indexed_relationship_types=["REL2"],
            batch_size=200,
        )

        assert job_id == "my-job"
        assert projection_runner is qr  # CollectingQueryRunner.cloneWithoutRouting returns self

        assert len(qr.queries) == 1
        assert "gds.arrow.project.v3" in qr.queries[0]
        assert qr.params[0] == {
            "graph_name": "myGraph",
            "query": "MATCH (n) RETURN n",
            "jobId": "my-job",
            "configuration": {
                "queryParameters": {"foo": "bar"},
                "undirectedRelationshipTypes": ["REL"],
                "inverseIndexedRelationshipTypes": ["REL2"],
                "concurrency": 4,
            },
            "arrow_config": {
                "host": "arrow.host",
                "port": 1234,
                "token": "some-token",
                "encrypted": True,
                "batchSize": 200,
            },
        }

    def test_start_cypher_projection_defaults_port_when_missing(
        self, arrow_client: MagicMock, qr: CollectingQueryRunner
    ) -> None:
        qr.add__mock_result(
            "gds.arrow.project.v3",
            DataFrame([{"host": "member-host", "other": "x"}]),
        )

        protocol = ProjectProtocolV3(arrow_client, qr, TerminationFlagNoop())

        job_id, _ = protocol.start_cypher_projection(
            graph_name="g",
            query="MATCH (n) RETURN n",
            job_id="my-job",
        )

        assert job_id == "my-job"
        assert len(qr.queries) == 1

    def test_get_status_replays_start_query(self, arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
        qr.add__mock_result(
            "gds.arrow.project.v3",
            [
                DataFrame([{"host": "member-host", "port": 7777}]),
                DataFrame([{"status": Status.RUNNING.name, "nodeCount": 0}]),
            ],
        )

        protocol = ProjectProtocolV3(arrow_client, qr, TerminationFlagNoop())

        _, projection_runner = protocol.start_cypher_projection(
            graph_name="g",
            query="MATCH (n) RETURN n",
            job_id="my-job",
        )

        status = protocol.get_status("my-job", projection_runner)

        assert status == {"status": Status.RUNNING.name, "nodeCount": 0}
        assert len(qr.queries) == 2
        assert all("gds.arrow.project.v3" in q for q in qr.queries)
        assert qr.params[0] == qr.params[1]

    def test_get_status_caches_done_result_and_does_not_requery(
        self, arrow_client: MagicMock, qr: CollectingQueryRunner
    ) -> None:
        qr.add__mock_result(
            "gds.arrow.project.v3",
            [
                DataFrame([{"host": "member-host", "port": 7777}]),
                DataFrame([{"status": Status.DONE.name, "nodeCount": 42}]),
            ],
        )

        protocol = ProjectProtocolV3(arrow_client, qr, TerminationFlagNoop())

        _, projection_runner = protocol.start_cypher_projection(
            graph_name="g",
            query="MATCH (n) RETURN n",
            job_id="my-job",
        )

        first = protocol.get_status("my-job", projection_runner)
        second = protocol.get_status("my-job", projection_runner)

        assert first == {"status": Status.DONE.name, "nodeCount": 42}
        assert second == first
        # Start + first status call only — second status call must hit the cache.
        assert len(qr.queries) == 2

    def test_start_cypher_projection_clears_cached_result_on_restart(
        self, arrow_client: MagicMock, qr: CollectingQueryRunner
    ) -> None:
        qr.add__mock_result(
            "gds.arrow.project.v3",
            [
                DataFrame([{"host": "member-host", "port": 7777}]),
                DataFrame([{"status": Status.DONE.name, "nodeCount": 1}]),
                DataFrame([{"host": "member-host", "port": 7777}]),
                DataFrame([{"status": Status.DONE.name, "nodeCount": 2}]),
            ],
        )

        protocol = ProjectProtocolV3(arrow_client, qr, TerminationFlagNoop())

        _, runner_1 = protocol.start_cypher_projection(graph_name="g", query="q", job_id="my-job")
        first = protocol.get_status("my-job", runner_1)

        _, runner_2 = protocol.start_cypher_projection(graph_name="g", query="q", job_id="my-job")
        second = protocol.get_status("my-job", runner_2)

        assert first == {"status": Status.DONE.name, "nodeCount": 1}
        assert second == {"status": Status.DONE.name, "nodeCount": 2}

    def test_start_store_projection_raises_not_implemented(
        self, arrow_client: MagicMock, qr: CollectingQueryRunner
    ) -> None:
        protocol = ProjectProtocolV3(arrow_client, qr, TerminationFlagNoop())

        with pytest.raises(NotImplementedError):
            protocol.start_store_projection(
                graph_name="g",
                node_label_filter=["Person"],
                relationship_type_filter=["KNOWS"],
            )


class TestProjectProtocolV4:
    def test_start_cypher_projection_dispatches_expected_query_and_params(
        self, arrow_client: MagicMock, qr: CollectingQueryRunner
    ) -> None:
        qr.add__mock_result(
            "gds.arrow.project.cypher.v4",
            DataFrame([{"jobId": "server-job", "host": "member-host", "port": 7777}]),
        )

        protocol = ProjectProtocolV4(arrow_client, qr, TerminationFlagNoop())

        job_id, projection_runner = protocol.start_cypher_projection(
            graph_name="myGraph",
            query="MATCH (n) RETURN n",
            job_id="my-job",
            query_parameters={"foo": "bar"},
            concurrency=4,
            undirected_relationship_types=["REL"],
            inverse_indexed_relationship_types=["REL2"],
            batch_size=200,
        )

        assert job_id == "server-job"
        assert projection_runner is qr

        assert len(qr.queries) == 1
        assert "gds.arrow.project.cypher.v4" in qr.queries[0]
        assert qr.params[0] == {
            "graph_name": "myGraph",
            "query": "MATCH (n) RETURN n",
            "jobId": "my-job",
            "configuration": {
                "queryParameters": {"foo": "bar"},
                "undirectedRelationshipTypes": ["REL"],
                "inverseIndexedRelationshipTypes": ["REL2"],
                "concurrency": 4,
            },
            "arrow_config": {
                "host": "arrow.host",
                "port": 1234,
                "token": "some-token",
                "encrypted": True,
                "batchSize": 200,
            },
        }

    def test_start_store_projection_dispatches_expected_query_and_params(
        self, arrow_client: MagicMock, qr: CollectingQueryRunner
    ) -> None:
        qr.add__mock_result(
            "gds.arrow.project.store.v4",
            DataFrame([{"jobId": "store-job", "host": "member-host", "port": 7777}]),
        )

        protocol = ProjectProtocolV4(arrow_client, qr, TerminationFlagNoop())

        job_id, projection_runner = protocol.start_store_projection(
            graph_name="myGraph",
            node_label_filter=["Person", "Movie"],
            relationship_type_filter=["ACTED_IN"],
            node_properties=["age"],
            relationship_properties=["weight"],
            job_id="store-job",
            concurrency=8,
            undirected_relationship_types=["ACTED_IN"],
            inverse_indexed_relationship_types=[],
            batch_size=50,
        )

        assert job_id == "store-job"
        assert projection_runner is qr

        assert len(qr.queries) == 1
        assert "gds.arrow.project.store.v4" in qr.queries[0]
        assert qr.params[0] == {
            "graph_name": "myGraph",
            "node_labels": ["Person", "Movie"],
            "relationship_types": ["ACTED_IN"],
            "configuration": {
                "nodeProperties": ["age"],
                "relationshipProperties": ["weight"],
                "jobId": "store-job",
                "undirectedRelationshipTypes": ["ACTED_IN"],
                "inverseIndexedRelationshipTypes": [],
                "readConcurrency": 8,
            },
            "arrow_config": {
                "host": "arrow.host",
                "port": 1234,
                "token": "some-token",
                "encrypted": True,
                "batchSize": 50,
            },
        }

    def test_start_defaults_port_when_missing(self, arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
        qr.add__mock_result(
            "gds.arrow.project.cypher.v4",
            DataFrame([{"jobId": "server-job", "host": "member-host"}]),
        )

        protocol = ProjectProtocolV4(arrow_client, qr, TerminationFlagNoop())

        job_id, _ = protocol.start_cypher_projection(graph_name="g", query="MATCH (n) RETURN n", job_id="my-job")

        assert job_id == "server-job"

    def test_get_status_dispatches_status_query(self, arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
        qr.add__mock_result(
            "gds.arrow.job.status",
            DataFrame([{"status": Status.RUNNING.name, "error": None, "result": None}]),
        )

        protocol = ProjectProtocolV4(arrow_client, qr, TerminationFlagNoop())

        status = protocol.get_status("server-job", qr)

        assert status == {"status": Status.RUNNING.name, "error": None, "result": None}
        assert len(qr.queries) == 1
        assert "gds.arrow.job.status.v4('server-job')" in qr.queries[0]

    def test_get_status_raises_when_error_present(self, arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
        qr.add__mock_result(
            "gds.arrow.job.status",
            DataFrame([{"status": Status.RUNNING.name, "error": "boom", "result": None}]),
        )

        protocol = ProjectProtocolV4(arrow_client, qr, TerminationFlagNoop())

        with pytest.raises(Exception, match="boom"):
            protocol.get_status("server-job", qr)
