import unittest
from unittest.mock import MagicMock

from graphdatascience.arrow_client.v2.api_types import MutateResult
from graphdatascience.arrow_client.v2.mutation_client import MutationClient
from graphdatascience.tests.unit.arrow_client.arrow_test_utils import ArrowTestResult


class TestMutationClient(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_client = MagicMock()

    def test_mutate_node_property_success(self) -> None:
        job_id = "test-job-123"
        expected_mutation_result = MutateResult(nodePropertiesWritten=42, relationshipsWritten=1337)

        self.mock_client.do_action_with_retry.return_value = iter(
            [ArrowTestResult(expected_mutation_result.dump_camel())]
        )

        result = MutationClient.mutate_node_property(self.mock_client, job_id, "propertyName")

        assert result == expected_mutation_result

        self.mock_client.do_action_with_retry.assert_called_once_with(
            MutationClient.MUTATE_ENDPOINT, b'{"jobId": "test-job-123", "mutateProperty": "propertyName"}'
        )
