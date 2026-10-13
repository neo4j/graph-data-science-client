# Changes in 2.2

## Breaking changes


## New features

- Added `compute` endpoints for GraphSage predictions. They start a prediction and return a `JobHandle` for streaming, mutating, or writing the result: `gds.graph_sage.compute` (classic GraphSage on a session), `gds.graph_sage.supervised.compute` and `gds.graph_sage.unsupervised.compute`.

## Bug fixes


## Improvements

- Improve progress logging for FastPath and supervised/unsupervised GraphSage


## Other changes
