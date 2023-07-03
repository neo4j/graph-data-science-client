import re
from itertools import chain, zip_longest
from typing import Any, Dict, Optional, Tuple

from pandas import Series

from ..query_runner.arrow_query_runner import ArrowQueryRunner
from ..query_runner.query_runner import QueryRunner
from ..server_version.server_version import ServerVersion
from .graph_object import Graph
from graphdatascience.caller_base import CallerBase


class GraphCypherRunner(CallerBase):
    def __init__(self, query_runner: QueryRunner, namespace: str, server_version: ServerVersion) -> None:
        if server_version < ServerVersion(2, 4, 0):
            raise ValueError("The new Cypher projection is only supported since GDS 2.4.0.")
        super().__init__(query_runner, namespace, server_version)

    def project(
        self,
        graph_name: str,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
    ) -> Tuple[Graph, "Series[Any]"]:
        """
        Run a Cypher projection.
        The provided query must end with a `RETURN gds.graph.project(...)` call.

        Parameters
        ----------
        graph_name: str
            the name of the graph to project
        query: str
            the Cypher projection query
        params: Dict[str, Any]
            parameters to the query
        database: str
            the database on which to run the query

        Returns
        -------
        A tuple of the projected graph and statistics about the projection
        """

        graph_name_param = GraphCypherRunner._find_return_clause_graph_name(self._namespace, query)

        if not graph_name_param.startswith("$"):
            raise ValueError(
                f"Invalid query, the `graph_name` must use a query parameter, but got `{graph_name_param}`: {query}"
            )

        graph_name_param = graph_name_param[1:]

        if params is not None and graph_name_param in params:
            query_graph_name = params[graph_name_param]
            if query_graph_name != graph_name:
                raise ValueError(
                    f"Invalid query, the `{graph_name_param}` parameter must be bound to `{graph_name}`: {query}"
                )
        else:
            if params is None:
                params = {}

            params[graph_name_param] = graph_name

        # See run_cypher
        qr = self._query_runner

        # The Arrow query runner should not be used to execute arbitrary Cypher
        if isinstance(qr, ArrowQueryRunner):
            qr = qr.fallback_query_runner()

        result = qr.run_query(query, params, database, False)
        result = result.squeeze()

        return Graph(graph_name, self._query_runner, self._server_version), result  # type: ignore

    __separators = re.compile(r"[,(.]")

    @staticmethod
    def _find_return_clause_graph_name(namespace: str, query: str) -> str:
        """
        Returns the 'graph name' in the RETURN clause of a Cypher projection query.
        'graph name' here is the first argument of the `gds.graph.project` function.
        """

        found = None
        at_end = False

        namespace_tokens = namespace.split(".")
        query_tokens = iter(query.split())

        for query_token in query_tokens:
            if at_end:
                raise ValueError(f"Invalid query, the query must end with the `RETURN {namespace}(...)` call: {query}")

            if found is not None:
                paren_balance = 0

                chars = chain(query_token, (c for tok in query_tokens for c in tok))

                for c in chars:
                    if c == "(":
                        paren_balance += 1
                    elif c == ")":
                        paren_balance -= 1
                        if paren_balance < 0:
                            at_end = True
                            break

            if query_token == "RETURN":
                tokens = (tok for token in query_tokens for tok in GraphCypherRunner.__separators.split(token) if tok)
                for token, expected in zip_longest(tokens, namespace_tokens):
                    if expected is None:
                        found = token
                        break

                    if token != expected:
                        break

        if found is None or not at_end:
            raise ValueError(
                f"Invalid query, the query must contain exactly one `RETURN {namespace}(...)` call: {query}"
            )

        return found
