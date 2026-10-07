# Changes in 2.2

## Breaking changes


## New features

- Added `compute` endpoints for GraphSage predictions. They start a prediction and return a `JobHandle` for streaming, mutating, or writing the result: `gds.graph_sage.compute` (classic GraphSage on a session), `gds.graph_sage.supervised.compute` and `gds.graph_sage.unsupervised.compute`.
- Graphs created in a session are now automatically registered as graph mappings, making them interoperable with the Cypher API: graphs created by either API under the same database user are visible in `gds.graph.list()` of the other, and mappings are cleaned up when a graph is dropped.

## Bug fixes


## Improvements


## Other changes
