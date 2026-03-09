from enum import Enum


# following https://neo4j.com/docs/operations-manual/current/monitoring/logging/#attach-metadata-tx
class QueryType(Enum):
    SYSTEM = "system"  # a query automatically run by the app.
    USER_DIRECTED = "user-direct"  # a query the user directly submitted to/through the app.
    USER_ACTION = "user-action"  # a query resulting from an action the user performed.
    USER_TRANSPILED = "user-transpiled"  # a query that has been derived from the user input.
