import logging
from abc import ABC, abstractmethod
from typing import Any, Optional

from pandas import DataFrame
from tenacity import retry, retry_if_result, wait_incrementing

from graphdatascience.call_parameters import CallParameters
from graphdatascience.query_runner.progress.progress_bar import TqdmProgressBar
from graphdatascience.query_runner.protocol.status import Status
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.termination_flag import TerminationFlag
from graphdatascience.retry_utils.retry_utils import before_log
from graphdatascience.session.dbms.protocol_version import ProtocolVersion


class WriteProtocol(ABC):
    @abstractmethod
    def write_back_params(
        self,
        graph_name: str,
        job_id: str,
        config: dict[str, Any],
        arrow_config: dict[str, Any],
        database: Optional[str] = None,
    ) -> CallParameters:
        """Transforms the given parameters into CallParameters that correspond to the right protocol version."""
        pass

    @abstractmethod
    def run_write_back(
        self,
        query_runner: QueryRunner,
        parameters: CallParameters,
        yields: Optional[list[str]],
        log_progress: bool,
        terminationFlag: TerminationFlag,
    ) -> DataFrame:
        """Executes the write-back procedure"""
        pass

    @staticmethod
    def select(protocol_version: ProtocolVersion) -> "WriteProtocol":
        return {
            ProtocolVersion.V1: RemoteWriteBackV1(),
            ProtocolVersion.V2: RemoteWriteBackV2(),
            ProtocolVersion.V3: RemoteWriteBackV3(),
        }[protocol_version]


class RemoteWriteBackV1(WriteProtocol):
    def write_back_params(
        self,
        graph_name: str,
        job_id: str,
        config: dict[str, Any],
        arrow_config: dict[str, Any],
        database: Optional[str] = None,
    ) -> CallParameters:
        return CallParameters(
            graphName=graph_name,
            databaseName=database,
            jobId=job_id,
            arrowConfiguration=arrow_config,
        )

    def run_write_back(
        self,
        query_runner: QueryRunner,
        parameters: CallParameters,
        yields: Optional[list[str]],
        log_progress: bool,
        terminationFlag: TerminationFlag,
    ) -> DataFrame:
        return query_runner.call_procedure(
            ProtocolVersion.V1.versioned_procedure_name("gds.arrow.write"),
            parameters,
            yields,
            retryable=False,
            database=None,
            logging=False,
            mode=QueryMode.WRITE,
            custom_error=False,
        )


class RemoteWriteBackV2(WriteProtocol):
    def write_back_params(
        self,
        graph_name: str,
        job_id: str,
        config: dict[str, Any],
        arrow_config: dict[str, Any],
        database: Optional[str] = None,
    ) -> CallParameters:
        configuration = {}

        if "concurrency" in config:
            configuration["concurrency"] = config["concurrency"]

        return CallParameters(
            graphName=graph_name,
            jobId=job_id,
            arrowConfiguration=arrow_config,
            configuration=configuration,
        )

    def run_write_back(
        self,
        query_runner: QueryRunner,
        parameters: CallParameters,
        yields: Optional[list[str]],
        log_progress: bool,
        terminationFlag: TerminationFlag,
    ) -> DataFrame:
        return query_runner.call_procedure(
            ProtocolVersion.V2.versioned_procedure_name("gds.arrow.write"),
            parameters,
            yields,
            retryable=False,
            database=None,
            logging=False,
            mode=QueryMode.WRITE,
            custom_error=False,
        )


class RemoteWriteBackV3(WriteProtocol):
    def __init__(self, progress_bar_options: Optional[dict[str, Any]] = None):
        self._progress_bar_options = progress_bar_options or {}

    def write_back_params(
        self,
        graph_name: str,
        job_id: str,
        config: dict[str, Any],
        arrow_config: dict[str, Any],
        database: Optional[str] = None,
    ) -> CallParameters:
        return RemoteWriteBackV2().write_back_params(graph_name, job_id, config, arrow_config, database)

    def run_write_back(
        self,
        query_runner: QueryRunner,
        parameters: CallParameters,
        yields: Optional[list[str]],
        log_progress: bool,
        terminationFlag: TerminationFlag,
    ) -> DataFrame:
        def is_not_completed(result: DataFrame) -> bool:
            status: str = result.iloc[0]["status"]
            return status != Status.COMPLETED.name

        logger = logging.getLogger()

        @retry(
            reraise=True,
            retry=retry_if_result(is_not_completed),
            wait=wait_incrementing(start=0.2, increment=0.2, max=2),
            before=before_log(
                f"Write-Back (graph: `{parameters['graphName']}`, jobId: `{parameters['jobId']}`)",
                logger,
                logging.DEBUG,
            ),
        )
        def write_fn(progress_bar: Optional[TqdmProgressBar]) -> DataFrame:
            terminationFlag.assert_running()
            result = query_runner.call_procedure(
                ProtocolVersion.V3.versioned_procedure_name("gds.arrow.write"),
                parameters,
                yields,
                retryable=True,
                logging=False,
                mode=QueryMode.WRITE,
                custom_error=False,
            )
            result_row = result.iloc[0].to_dict()
            # for self-managed dbs the endpoint doesn't return the progress yet
            progress = result_row.get("progress", 0.0) * 100

            if progress_bar:
                progress_bar.update(status=result_row["status"], progress=progress)

            return result

        if log_progress:
            with TqdmProgressBar(
                task_name=f"Write-Back (graph: {parameters['graphName']})",
                relative_progress=0.0,
                bar_options=self._progress_bar_options,
            ) as progress_bar:
                return write_fn(progress_bar)
        else:
            return write_fn(None)
