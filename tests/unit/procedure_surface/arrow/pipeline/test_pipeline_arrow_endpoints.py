from unittest import mock

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.pipeline.pipeline_arrow_endpoints import PipelineArrowEndpoints


def test_pipeline_arrow_list_runs_list_action() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    row = mock.Mock()
    row.body.to_pybytes.return_value = b'{"pipelineName":"pipe","pipelineType":"Node classification training pipeline"}'
    arrow_client.do_action_with_retry.return_value = [row]

    result = PipelineArrowEndpoints(arrow_client, None).list("pipe")

    assert len(result) == 1
    assert result[0].pipeline_name == "pipe"
    arrow_client.do_action_with_retry.assert_called_once_with("v2/pipeline.list", {"pipelineName": "pipe"})
