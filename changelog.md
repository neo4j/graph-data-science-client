# Changes in 2.4

## Breaking changes


## New features


## Bug fixes

* Fixed `gds.graph.relationships.write` ignoring the `write_concurrency` parameter when running against a Neo4j database with GDS installed.

## Improvements

- `QueryType`, `AlgorithmCategory`, `ArrowEndpointVersion`, and `ProtocolVersion` are now
  `StrEnum`s: `str()` of a member returns the underlying value (for example,
  `str(QueryType.USER_ACTION)` is `"user-action"` instead of `"QueryType.USER_ACTION"`),
  and members compare equal to their string values.

## Other changes
