from typing import Generator

import pandas as pd
import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.write_job_handle import JobNotFinishedError, WriteJobHandle
from graphdatascience.query_runner.protocol.write_protocols import WriteProtocol
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph, create_graph_from_db

GDL = """
    CREATE
        (a:Node {id: 0}),
        (b:Node {id: 1}),
        (c:Node {id: 2}),
        (a)-[:REL]->(c),
        (b)-[:REL]->(c)
    """


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, "g", GDL) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    with create_graph_from_db(
        arrow_client,
        query_runner,
        "g",
        GDL,
        """
            MATCH (n)-->(m)
            WITH gds.graph.project.remote(n, m, {sourceNodeProperties: properties(n), targetNodeProperties: properties(m)}) as g
            RETURN g
        """,
    ) as g:
        yield g


def _start_pagerank(arrow_client: AuthenticatedArrowClient, graph: GraphV2) -> str:
    return JobClient.run_job(arrow_client, "v2/centrality.pageRank", {"graphName": graph.name()})


def _make_handle(
    arrow_client: AuthenticatedArrowClient,
    graph: GraphV2,
    *,
    write_protocol: WriteProtocol | None = None,
) -> JobHandle:
    job_id = _start_pagerank(arrow_client, graph)
    return JobHandle(
        arrow_client=arrow_client,
        write_protocol=write_protocol,
        job_id=job_id,
        graph=graph,
        show_progress=False,
        endpoint="v2/centrality.pageRank",
    )


def test_job_id_matches_started_job(arrow_client: AuthenticatedArrowClient, sample_graph: GraphV2) -> None:
    job_id = _start_pagerank(arrow_client, sample_graph)
    handle = JobHandle(
        arrow_client=arrow_client,
        write_protocol=None,
        job_id=job_id,
        graph=sample_graph,
        show_progress=False,
        endpoint="v2/centrality.pageRank",
    )

    assert handle.job_id() == job_id


def test_wait_makes_handle_done(arrow_client: AuthenticatedArrowClient, sample_graph: GraphV2) -> None:
    handle = _make_handle(arrow_client, sample_graph)

    handle.wait()

    assert handle.done() is True


def test_status_returns_terminal_after_wait(arrow_client: AuthenticatedArrowClient, sample_graph: GraphV2) -> None:
    handle = _make_handle(arrow_client, sample_graph)
    handle.wait()

    status = handle.status()

    assert status.succeeded()


def test_summary_waits_and_returns_dict_with_internals_stripped(
    arrow_client: AuthenticatedArrowClient, sample_graph: GraphV2
) -> None:
    handle = _make_handle(arrow_client, sample_graph)

    summary = handle.summary()

    assert summary["computeMillis"] >= 0
    config = summary["configuration"]
    assert "writeProperty" not in config
    assert "writeConcurrency" not in config


def test_summary_no_wait_raises_when_not_done(arrow_client: AuthenticatedArrowClient, sample_graph: GraphV2) -> None:
    job_id = JobClient.run_job(arrow_client, "v2/centrality.pageRank", {"graphName": sample_graph.name()})

    handle = JobHandle(
        arrow_client=arrow_client,
        write_protocol=None,
        job_id=job_id,
        graph=sample_graph,
        show_progress=False,
        endpoint="v2/centrality.pageRank",
    )

    # Best-effort: if the tiny pagerank finished too quickly, just skip this assertion.
    if not handle.done():
        with pytest.raises(JobNotFinishedError):
            handle.summary(wait=False)


def test_stream_returns_dataframe(arrow_client: AuthenticatedArrowClient, sample_graph: GraphV2) -> None:
    handle = _make_handle(arrow_client, sample_graph)

    df = handle.stream()

    assert isinstance(df, pd.DataFrame)
    assert "nodeId" in df.columns
    assert "score" in df.columns
    assert len(df) == 3


def test_mutate_writes_node_property(arrow_client: AuthenticatedArrowClient, sample_graph: GraphV2) -> None:
    handle = _make_handle(arrow_client, sample_graph)

    result = handle.mutate(mutate_property="pagerank")

    assert result["nodePropertiesWritten"] == 3
    assert result["configuration"]["mutateProperty"] == "pagerank"


def test_mutate_without_target_raises(arrow_client: AuthenticatedArrowClient, sample_graph: GraphV2) -> None:
    handle = _make_handle(arrow_client, sample_graph)

    with pytest.raises(ValueError, match="Provide one of"):
        handle.mutate()


def test_write_without_protocol_raises(arrow_client: AuthenticatedArrowClient, sample_graph: GraphV2) -> None:
    handle = _make_handle(arrow_client, sample_graph)

    with pytest.raises(ValueError, match="does not support write operations"):
        handle.write(write_properties="pagerank")


@pytest.mark.db_integration
def test_write_returns_write_job_handle(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: GraphV2
) -> None:
    write_protocol = WriteProtocol.select(arrow_client, query_runner)
    handle = _make_handle(arrow_client, db_graph, write_protocol=write_protocol)
    handle.wait()

    write_handle = handle.write(write_properties="pagerank")

    assert isinstance(write_handle, WriteJobHandle)
    result = write_handle.result()
    assert result.written_node_properties == 3
