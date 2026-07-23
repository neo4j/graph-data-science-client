from contextlib import contextmanager
from typing import Iterator
from unittest.mock import MagicMock, patch

from gds_cli.common.env import DatabaseConfig
from gds_cli.database.fetch import download_graph

_DB = DatabaseConfig(uri="bolt://localhost", username="neo4j", password="pw", database="neo4j")


@contextmanager
def _fake_driver(session: MagicMock) -> Iterator[MagicMock]:
    driver = MagicMock()
    driver.session.return_value.__enter__.return_value = session
    driver.session.return_value.__exit__.return_value = False
    yield driver


def test_download_graph_passes_labels_as_query_parameter() -> None:
    session = MagicMock()
    session.run.return_value.data.return_value = []

    # A label carrying a quote would break/inject a string-interpolated query;
    # it must instead ride along as the `labels` parameter, leaving the Cypher fixed.
    malicious = 'Person" OR true OR "'
    with patch("gds_cli.database.fetch.open_driver", return_value=_fake_driver(session)):
        download_graph(_DB, node_labels=[malicious])

    assert session.run.call_count == 2  # nodes query + relationships query
    for call in session.run.call_args_list:
        query = call.args[0]
        assert call.kwargs == {"labels": [malicious]}  # passed as a parameter
        assert "$labels" in query  # filtered via the parameter
        assert malicious not in query  # user input never interpolated into Cypher


def test_download_graph_without_labels_sends_no_params() -> None:
    session = MagicMock()
    session.run.return_value.data.return_value = []

    with patch("gds_cli.database.fetch.open_driver", return_value=_fake_driver(session)):
        download_graph(_DB, node_labels=None)

    for call in session.run.call_args_list:
        assert call.kwargs == {}
        assert "$labels" not in call.args[0]
