import re

import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.server_version.compatible_with import (
    IncompatibleServerVersionError,
)
from graphdatascience.server_version.server_version import ServerVersion


@pytest.mark.parametrize("server_version", [ServerVersion(2, 2, 0)])  # Something later than 2.1.0
def test_incompatible_server_version(runner: QueryRunner, gds: GraphDataScience) -> None:
    G = Graph("dummy", runner, ServerVersion(0, 0, 0))  # This version object is not relevant for the test
    with pytest.raises(
        IncompatibleServerVersionError,
        match=re.escape(
            f"The call gds.graph.removeNodeProperties with parameters ['G', 'node_properties', "
            f"'node_labels', 'config'] requires GDS server version < 2.1.0. The current version is"
            f" {gds._server_version}"
        ),
    ):
        gds.graph.removeNodeProperties(G, ["dummyProp"], "dummyLabel", concurrency=2)
