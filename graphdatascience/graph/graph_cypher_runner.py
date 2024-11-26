from __future__ import annotations

import re
from itertools import chain, zip_longest
from typing import Any, Optional

from pandas import Series

from ..caller_base import CallerBase
from ..query_runner.query_runner import QueryRunner
from ..server_version.server_version import ServerVersion
from .graph_create_result import GraphCreateResult
from .graph_object import Graph


class GraphCypherRunner(CallerBase):
    def __init__(self, query_runner: QueryRunner, namespace: str, server_version: ServerVersion) -> None:
        if server_version < ServerVersion(2, 4, 0):
            raise ValueError("The new Cypher projection is only supported since GDS 2.4.0.")
        super().__init__(query_runner, namespace, server_version)

    def project(
        self,
        query: str,
        database: Optional[str] = None,
        **params: Any,
    ) -> GraphCreateResult:
        """
        Run a Cypher projection.
        The provided query must end with a `RETURN gds.graph.project(...)` call.

        Parameters
        ----------
        query: str
            the Cypher projection query
        database: str
            the database on which to run the query
        params: Dict[str, Any]
            parameters to the query

        Returns
        -------
        A tuple of the projected graph and statistics about the projection
        """

        GraphCypherRunner._verify_query_ends_with_return_clause(self._namespace, query)

        result: Optional[dict[str, Any]] = self._query_runner.run_cypher(query, params, database, False).squeeze()

        if not result:
            raise ValueError("Projected graph cannot be empty.")

        try:
            graph_name = str(result["graphName"])
        except (KeyError, TypeError):
            raise ValueError(
                f"Invalid query, the query must end with the `RETURN {self._namespace}(...)` call: {query}"
            )

        return GraphCreateResult(Graph(graph_name, self._query_runner), Series(data=result))

    __separators = re.compile(r"[,(.]")

    @staticmethod
    def _verify_query_ends_with_return_clause(namespace: str, query: str) -> None:
        """
        Verifies that the query ends in a `RETURN gds.graph.project(...)` call.
        Invalid queries will raise a ValueError.
        """

        # We iterate through the "tokens" of the query, where a token here is
        # the result of splitting on whitespace.
        # The loop body is basically a state machine with three states:
        # 1. Finding the start of the `RETURN gds.graph.project` call (found = False, at_end = False)
        # 2. Finding the end of that call (found = True, at_end = False)
        # 3. Verify we are at the end (found = <any>, at_end = True)
        found = False
        at_end = False

        namespace_tokens = namespace.split(".")
        query_tokens = iter(query.split())

        for query_token in query_tokens:
            if at_end:
                # State 3: Nothing is allowed when we are at the end
                raise ValueError(f"Invalid query, the query must end with the `RETURN {namespace}(...)` call: {query}")

            if found:
                # State 2: We are in the `RETURN gds.graph.project` call.
                # Find closing parenthesis of the call by going through each character
                # and keeping track of the parenthesis balance.
                chars = chain(query_token, (c for tok in query_tokens for c in tok))
                paren_balance = 0

                for c in chars:
                    if c == "(":
                        paren_balance += 1
                    elif c == ")":
                        paren_balance -= 1
                        if paren_balance < 0:
                            at_end = True
                            break

            if query_token == "RETURN":
                # State 1: We found the start of a `RETURN` clause.
                # Check if it is the `RETURN gds.graph.project` call.
                # We split tokens on `__separators` and flatten the nested iters.
                tokens = (tok for token in query_tokens for tok in GraphCypherRunner.__separators.split(token) if tok)
                for token, expected in zip_longest(tokens, namespace_tokens):
                    if expected is None:
                        # First token after the correct namespace, we are now _inside_ the arguments
                        found = True
                        break

                    if token != expected:
                        break

        if not found or not at_end:
            raise ValueError(
                f"Invalid query, the query must contain exactly one `RETURN {namespace}(...)` call: {query}"
            )
