import warnings
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Any, Dict, Optional
from uuid import uuid4

import neo4j
from pandas.core.frame import DataFrame
from tqdm.auto import tqdm

from ..error.unable_to_connect import UnableToConnectError
from .graph_constructor import GraphConstructor
from .query_runner import QueryRunner


class Neo4jQueryRunner(QueryRunner):
    _LOG_POLLING_INTERVAL = 0.5

    def __init__(self, driver: neo4j.Driver, db: Optional[str] = neo4j.DEFAULT_DATABASE, auto_close: bool = False):
        self._driver = driver
        self._auto_close = auto_close

        if db:
            self._db = db
            return

        try:
            with self._driver.session() as session:
                result = session.run("SHOW DATABASES YIELD name, default WHERE default = TRUE RETURN name")
                self._db = result.data()[0]["name"]
        except Exception as e:
            raise UnableToConnectError(e)

    def run_query(self, query: str, params: Dict[str, Any] = {}) -> DataFrame:
        with self._driver.session(database=self._db) as session:
            result = session.run(query, params)

            # Though pandas support may be experimental in the `neo4j` package, it should always
            # be supported in the `graphdatascience` package.
            warnings.filterwarnings(
                "ignore",
                message=r"^pandas support is experimental and might be changed or removed in future versions$",
            )

            return result.to_df()  # type: ignore

    def run_query_with_logging(self, query: str, params: Dict[str, Any] = {}) -> DataFrame:
        if "config" in params:
            if "jobId" in params["config"]:
                job_id = params["config"]["jobId"]
            else:
                job_id = str(uuid4())
                params["config"]["jobId"] = job_id
        else:
            job_id = str(uuid4())
            params["config"] = {"jobId": job_id}

        with ThreadPoolExecutor() as executor:
            future = executor.submit(self.run_query, query, params)

            self._log(job_id, future)

            if future.exception():
                raise future.exception()  # type: ignore
            else:
                return future.result()

    def _log(self, job_id: str, future: "Future[Any]") -> None:
        pbar = tqdm(total=100, unit="%")

        while wait([future], timeout=self._LOG_POLLING_INTERVAL).not_done:
            try:
                progress = self.run_query(f"CALL gds.beta.listProgress('{job_id}') YIELD taskName, progress")

                parsed_name = progress["taskName"][0].split("|--")[-1][1:]
                pbar.set_description(parsed_name)

                progress_num = float(progress["progress"][0][:-1])
                pbar.update(progress_num - pbar.n)
            except Exception as e:
                # Do nothing if the procedure either:
                # * has not started yet,
                # * has already completed,
                # * or it simply does not support progress logging.
                if f"No task with job id `{job_id}` was found" not in str(e):
                    raise e

        pbar.update(100 - pbar.n)

    def set_database(self, db: str) -> None:
        self._db = db

    def close(self) -> None:
        self._driver.close()

    def database(self) -> str:
        return self._db

    def __del__(self) -> None:
        if self._auto_close:
            self._driver.close()

    def create_graph_constructor(self, _: str, __: int) -> GraphConstructor:
        raise ValueError("This feature requires the GDS Flight server to be enabled.")
