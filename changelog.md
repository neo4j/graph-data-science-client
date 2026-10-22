# Changes in 2.3

## Breaking changes


## New features

* `GdsSessions.get_or_create` can attach a session to a virtual graph or composite instance.
  The instance type is detected from the connected database.

## Bug fixes


## Improvements

* A `uri` given in `DbmsConnectionInfo` is used as-is for AuraDB connections, instead of being replaced by the instance's connection URL.
* The instance type is detected before the instance is resolved through the Aura API, so attaching to a virtual graph or composite no longer requires an extra API call.
* `gds.graph.project.cypher` reports that no graph was created if the projection query matches no data, instead of a misleading error that the query must end with a `RETURN gds.graph.project(...)` call.


## Other changes

* Drop support for Python 3.10
* Support Python 3.15
* The supported `pyarrow` range is now `>= 22.0, < 27.0`.
