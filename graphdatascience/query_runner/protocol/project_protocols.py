from abc import ABC, abstractmethod
from logging import DEBUG, getLogger
from typing import Any, Optional

from pandas import DataFrame
from tenacity import retry, retry_if_result, wait_incrementing

from graphdatascience import QueryRunner
from graphdatascience.call_parameters import CallParameters
from graphdatascience.query_runner.protocol.status import Status
from graphdatascience.query_runner.termination_flag import TerminationFlag
from graphdatascience.retry_utils.retry_utils import before_log
from graphdatascience.session.dbms.protocol_version import ProtocolVersion


class ProjectProtocol(ABC):
    @abstractmethod
    def project_params(
        self, graph_name: str, query: str, job_id: str, params: dict[str, Any], arrow_config: dict[str, Any]
    ) -> CallParameters:
        """Transforms the given parameters into CallParameters that correspond to the right protocol version."""
        pass

    @abstractmethod
    def run_projection(
        self,
        query_runner: QueryRunner,
        endpoint: str,
        params: CallParameters,
        terminationFlag: TerminationFlag,
        yields: Optional[list[str]] = None,
        database: Optional[str] = None,
        logging: bool = False,
    ) -> DataFrame:
        """Returns the procedure name for the corresponding protocol version."""
        pass

    @staticmethod
    def select(protocol_version: ProtocolVersion) -> "ProjectProtocol":
        return {
            ProtocolVersion.V1: ProjectProtocolV1(),
            ProtocolVersion.V2: ProjectProtocolV2(),
            ProtocolVersion.V3: ProjectProtocolV3(),
        }[protocol_version]


class ProjectProtocolV1(ProjectProtocol):
    def project_params(
        self, graph_name: str, query: str, job_id: str, params: dict[str, Any], arrow_config: dict[str, Any]
    ) -> CallParameters:
        return CallParameters(
            graph_name=graph_name,
            query=query,
            concurrency=params["concurrency"],
            undirected_relationship_types=params["undirected_relationship_types"],
            inverse_indexed_relationship_types=params["inverse_indexed_relationship_types"],
            arrow_configuration=arrow_config,
        )

    def run_projection(
        self,
        query_runner: QueryRunner,
        endpoint: str,
        params: CallParameters,
        terminationFlag: TerminationFlag,
        yields: Optional[list[str]] = None,
        database: Optional[str] = None,
        logging: bool = False,
    ) -> DataFrame:
        versioned_endpoint = ProtocolVersion.V1.versioned_procedure_name(endpoint)
        return query_runner.call_procedure(versioned_endpoint, params, yields, database, logging, False)


class ProjectProtocolV2(ProjectProtocol):
    def project_params(
        self, graph_name: str, query: str, job_id: str, params: dict[str, Any], arrow_config: dict[str, Any]
    ) -> CallParameters:
        return CallParameters(
            graph_name=graph_name,
            query=query,
            arrow_configuration=arrow_config,
            configuration={
                "concurrency": params["concurrency"],
                "undirectedRelationshipTypes": params["undirected_relationship_types"],
                "inverseIndexedRelationshipTypes": params["inverse_indexed_relationship_types"],
            },
        )

    def run_projection(
        self,
        query_runner: QueryRunner,
        endpoint: str,
        params: CallParameters,
        terminationFlag: TerminationFlag,
        yields: Optional[list[str]] = None,
        database: Optional[str] = None,
        logging: bool = False,
    ) -> DataFrame:
        versioned_endpoint = ProtocolVersion.V2.versioned_procedure_name(endpoint)
        return query_runner.call_procedure(versioned_endpoint, params, yields, database, logging, False)


class ProjectProtocolV3(ProjectProtocol):
    def project_params(
        self, graph_name: str, query: str, job_id: str, params: dict[str, Any], arrow_config: dict[str, Any]
    ) -> CallParameters:
        return CallParameters(
            graph_name=graph_name,
            query=query,
            job_id=job_id,
            arrow_configuration=arrow_config,
            configuration={
                "concurrency": params["concurrency"],
                "undirectedRelationshipTypes": params["undirected_relationship_types"],
                "inverseIndexedRelationshipTypes": params["inverse_indexed_relationship_types"],
            },
        )

    def run_projection(
        self,
        query_runner: QueryRunner,
        endpoint: str,
        params: CallParameters,
        terminationFlag: TerminationFlag,
        yields: Optional[list[str]] = None,
        database: Optional[str] = None,
        logging: bool = False,
    ) -> DataFrame:
        def is_not_done(result: DataFrame) -> bool:
            status: str = result.squeeze()["status"]
            return status != Status.DONE.name

        logger = getLogger()

        @retry(
            reraise=True,
            before=before_log(f"Projection (graph: `{params['graph_name']}`)", logger, DEBUG),
            retry=retry_if_result(is_not_done),
            wait=wait_incrementing(start=0.2, increment=0.2, max=2),
        )
        def project_fn() -> DataFrame:
            terminationFlag.assert_running()
            return query_runner.call_procedure(
                ProtocolVersion.V3.versioned_procedure_name(endpoint), params, yields, database, logging, False
            )

        return project_fn()
