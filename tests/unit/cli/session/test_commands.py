from unittest.mock import MagicMock, patch

from gds_cli.session.commands import _connect
from gds_cli.session.config import AlgorithmConfig, JobConfig, ProjectionConfig, SessionConfig
from gds_cli.session.report import JobReport


def _cfg() -> JobConfig:
    return JobConfig(
        session=SessionConfig(name="my-session", memory="2GB", ttl_minutes=30),
        projections=[ProjectionConfig(graph_name="social", query="MATCH (n) RETURN gds.graph.project.remote(n, n)")],
        algorithms=[AlgorithmConfig(name="louvain", graph_name="social", mode="mutate", mutate_property="community")],
    )


def _session_info(status: str, session_id: str) -> MagicMock:
    info = MagicMock()
    info.status = status
    info.id = session_id
    return info


def test_connect_deletes_and_recreates_a_deleted_session() -> None:
    cfg = _cfg()
    fake_gds = MagicMock()

    with (
        patch("gds_cli.session.session_ops.find_session") as fake_find,
        patch("gds_cli.session.session_ops.delete") as fake_delete,
        patch("gds_cli.session.session_ops.connect", return_value=fake_gds) as fake_connect,
    ):
        fake_find.side_effect = [_session_info("Deleting", "old-id"), _session_info("Creating", "new-id")]
        gds = _connect(cfg, JobReport())

    fake_delete.assert_called_once_with(cfg.session)
    fake_connect.assert_called_once()
    assert gds is fake_gds


def test_connect_does_not_delete_a_healthy_existing_session() -> None:
    cfg = _cfg()
    fake_gds = MagicMock()

    with (
        patch("gds_cli.session.session_ops.find_session", return_value=_session_info("Ready", "id-1")),
        patch("gds_cli.session.session_ops.delete") as fake_delete,
        patch("gds_cli.session.session_ops.connect", return_value=fake_gds),
    ):
        _connect(cfg, JobReport())

    fake_delete.assert_not_called()


def test_connect_recreates_even_if_cleanup_delete_fails() -> None:
    cfg = _cfg()
    fake_gds = MagicMock()

    with (
        patch("gds_cli.session.session_ops.find_session") as fake_find,
        patch("gds_cli.session.session_ops.delete", side_effect=RuntimeError("boom")),
        patch("gds_cli.session.session_ops.connect", return_value=fake_gds) as fake_connect,
    ):
        fake_find.side_effect = [_session_info("Deleted", "old-id"), None]
        gds = _connect(cfg, JobReport())

    fake_connect.assert_called_once()
    assert gds is fake_gds
