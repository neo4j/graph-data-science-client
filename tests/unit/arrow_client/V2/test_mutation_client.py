from pytest_mock import MockerFixture

from graphdatascience.arrow_client.v2.mutation_client import MutationClient
from graphdatascience.tests.unit.arrow_client.arrow_test_utils import ArrowTestResult


def test_mutate_node_property_success(mocker: MockerFixture) -> None:
    job_id = "test-job-123"
    arrow_mutation_result = {"nodePropertiesWritten": 42, "relationshipsWritten": 1337}

    mock_client = mocker.Mock()
    mock_client.do_action_with_retry.return_value = iter([ArrowTestResult(arrow_mutation_result)])

    result = MutationClient.mutate_node_property(mock_client, job_id, "propertyName")

    assert result.node_properties_written == 42
    assert result.relationships_written == 1337
    assert result.mutate_millis > 0

    args, _ = mock_client.do_action_with_retry.call_args

    assert args[0] == MutationClient.MUTATE_ENDPOINT
    assert args[1] == {"jobId": "test-job-123", "mutateProperty": "propertyName"}
