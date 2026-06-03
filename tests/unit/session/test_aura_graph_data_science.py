import re

import pytest
from pandas import DataFrame
from pytest_mock import MockerFixture

from graphdatascience import ServerVersion
from graphdatascience.pipeline.lp_training_pipeline import LPTrainingPipeline
from graphdatascience.pipeline.nc_training_pipeline import NCTrainingPipeline
from graphdatascience.pipeline.nr_training_pipeline import NRTrainingPipeline
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.session_lifecycle_manager import SessionLifecycleManager
from graphdatascience.session.session_v2_endpoints import SessionV2Endpoints
from tests.unit.conftest import CollectingQueryRunner


def test_run_cypher_write(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        db_query_runner=None,
        session_lifecycle_manager=mocker.Mock(spec=SessionLifecycleManager),
        gds_version=v,
        v2_endpoints=mocker.Mock(),
        authenticated_arrow_client=mocker.Mock(),
    )

    gds.run_cypher("RETURN 1", params={"foo": 1}, mode=QueryMode.WRITE, database="bar", retryable=True)

    assert query_runner.last_query() == "RETURN 1"
    assert query_runner.last_params() == {"foo": 1}
    assert query_runner.run_args[-1] == {"custom_error": False, "db": "bar", "mode": QueryMode.WRITE, "retryable": True}


def test_run_cypher_read(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        db_query_runner=None,
        session_lifecycle_manager=mocker.Mock(spec=SessionLifecycleManager),
        gds_version=v,
        v2_endpoints=mocker.Mock(),
        authenticated_arrow_client=mocker.Mock(),
    )

    gds.run_cypher("RETURN 1", params={"foo": 1}, mode=QueryMode.READ, retryable=False)

    assert query_runner.last_query() == "RETURN 1"
    assert query_runner.last_params() == {"foo": 1}
    assert query_runner.run_args[-1] == {
        "custom_error": False,
        "db": None,
        "mode": QueryMode.READ,
        "retryable": False,
        "query_type": "user-direct",
    }


def test_verify_connectivity(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = mocker.Mock(spec=Neo4jQueryRunner)
    session_lifecycle_manager = mocker.Mock(spec=SessionLifecycleManager)
    v2_endpoints = mocker.Mock(spec=SessionV2Endpoints)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        db_query_runner=None,
        session_lifecycle_manager=session_lifecycle_manager,
        gds_version=v,
        v2_endpoints=v2_endpoints,
        authenticated_arrow_client=mocker.Mock(),
    )

    gds.verify_connectivity()

    session_lifecycle_manager.verify_health.assert_called_once()
    v2_endpoints.verify_session_connectivity.assert_called_once()
    v2_endpoints.verify_db_connectivity.assert_called_once()


def test_delete(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    session_lifecycle_manager = mocker.Mock(spec=SessionLifecycleManager)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        db_query_runner=query_runner,
        session_lifecycle_manager=session_lifecycle_manager,
        gds_version=v,
        v2_endpoints=mocker.Mock(),
        authenticated_arrow_client=mocker.Mock(),
    )

    gds.delete()

    session_lifecycle_manager.delete.assert_called_once()


def test_session_pipeline_catalog_calls_warn(runner: CollectingQueryRunner, aura_gds: AuraGraphDataScience) -> None:
    with pytest.warns(
        DeprecationWarning, match=re.escape("Deprecated `gds.pipeline.list` in favor of `gds.v2.pipeline.list`")
    ):
        aura_gds.pipeline.list()

    assert runner.last_query() == "CALL gds.pipeline.list()"

    with pytest.warns(
        DeprecationWarning,
        match=re.escape("Deprecated `gds.pipeline.exists` in favor of `gds.v2.pipeline.exists`"),
    ):
        aura_gds.pipeline.exists("my_pipeline")

    assert runner.last_query() == "CALL gds.pipeline.exists($pipeline_name)"

    with pytest.warns(
        DeprecationWarning, match=re.escape("Deprecated `gds.pipeline.drop` in favor of `gds.v2.pipeline.drop`")
    ):
        aura_gds.pipeline.drop(LPTrainingPipeline("pipe", runner, runner.server_version()))

    assert runner.last_query() == "CALL gds.pipeline.drop($pipeline_name)"


def test_session_pipeline_get_warns(runner: CollectingQueryRunner, aura_gds: AuraGraphDataScience) -> None:
    runner.add__mock_result(
        "gds.pipeline.exists",
        DataFrame(
            [
                {
                    "exists": True,
                    "pipelineName": "pipe",
                    "pipelineType": "Link prediction training pipeline",
                }
            ]
        ),
    )

    with pytest.warns(
        DeprecationWarning,
        match=re.escape("Deprecated `gds.pipeline.get` in favor of `gds.v2.pipeline.<pipeline_type>.get`"),
    ):
        pipeline = aura_gds.pipeline.get("pipe")

    assert isinstance(pipeline, LPTrainingPipeline)
    assert pipeline.name() == "pipe"
    assert runner.last_query() == "CALL gds.pipeline.exists($pipeline_name)"


def test_session_pipeline_create_helpers_warn(runner: CollectingQueryRunner, aura_gds: AuraGraphDataScience) -> None:
    with pytest.warns(
        DeprecationWarning,
        match=re.escape("Deprecated `gds.lp_pipe` in favor of `gds.v2.pipeline.link_prediction.create`"),
    ):
        lp = aura_gds.lp_pipe("lp")

    assert isinstance(lp, LPTrainingPipeline)
    assert runner.last_query() == "CALL gds.beta.pipeline.linkPrediction.create($name)"

    with pytest.warns(
        DeprecationWarning,
        match=re.escape("Deprecated `gds.nc_pipe` in favor of `gds.v2.pipeline.node_classification.create`"),
    ):
        nc = aura_gds.nc_pipe("nc")

    assert isinstance(nc, NCTrainingPipeline)
    assert runner.last_query() == "CALL gds.beta.pipeline.nodeClassification.create($name)"

    with pytest.warns(
        DeprecationWarning,
        match=re.escape("Deprecated `gds.nr_pipe` in favor of `gds.v2.pipeline.node_regression.create`"),
    ):
        nr = aura_gds.nr_pipe("nr")

    assert isinstance(nr, NRTrainingPipeline)
    assert runner.last_query() == "CALL gds.alpha.pipeline.nodeRegression.create($pipeline_name)"


def test_session_alpha_and_beta_pipeline_calls_warn(
    runner: CollectingQueryRunner, aura_gds: AuraGraphDataScience
) -> None:
    with pytest.warns(
        DeprecationWarning,
        match=re.escape("Deprecated `gds.beta.pipeline.list` in favor of `gds.v2.pipeline.list`"),
    ):
        aura_gds.beta.pipeline.list()

    assert runner.last_query() == "CALL gds.beta.pipeline.list()"

    with pytest.warns(
        DeprecationWarning,
        match=re.escape(
            "Deprecated `gds.beta.pipeline.linkPrediction.create` in favor of `gds.v2.pipeline.link_prediction.create`"
        ),
    ):
        lp, _ = aura_gds.beta.pipeline.linkPrediction.create("lp")

    assert isinstance(lp, LPTrainingPipeline)
    assert runner.last_query() == "CALL gds.beta.pipeline.linkPrediction.create($name)"

    with pytest.warns(
        DeprecationWarning,
        match=re.escape(
            "Deprecated `gds.alpha.pipeline.nodeRegression.create` in favor of `gds.v2.pipeline.node_regression.create`"
        ),
    ):
        nr, _ = aura_gds.alpha.pipeline.nodeRegression.create("nr")

    assert isinstance(nr, NRTrainingPipeline)
    assert runner.last_query() == "CALL gds.alpha.pipeline.nodeRegression.create($pipeline_name)"
