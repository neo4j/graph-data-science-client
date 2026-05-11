from unittest import mock

import pandas as pd

from graphdatascience.procedure_surface.cypher.pipeline.pipeline_cypher_endpoints import PipelineCypherEndpoints


def test_pipeline_cypher_list_runs_query() -> None:
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = pd.DataFrame(
        [{"pipelineName": "pipe", "pipelineType": "Node classification training pipeline"}]
    )

    result = PipelineCypherEndpoints(query_runner).list("pipe")

    assert len(result) == 1
    assert result[0].pipeline_name == "pipe"
    query_runner.call_procedure.assert_called_once_with(
        "gds.pipeline.list",
        params=mock.ANY,
        custom_error=False,
    )
    assert query_runner.call_procedure.call_args.kwargs["params"] == {"pipeline_name": "pipe"}
