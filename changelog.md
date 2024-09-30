# Changes in 1.12


## Breaking changes


## New features

* Add Neo4j python driver rust extension as a new optional dependency.
* Support creating GDS Sessions for self-managed Neo4j DBMS.
  * `GdsSessions.get_or_create` requires a new parameter `cloud_location` to specify where the session will be created.
* Return the id of a session and allow deletion by id or name.
* Add `ttl` parameter to `GdsSessions.get_or_create` to control if and when an unused session will be automatically deleted.
* Add concurrency control for remote write-back procedures using the `concurrency` parameter.
* Add progress logging for remote write-back when using GDS Sessions.
* Added a flag to GraphDataScience and AuraGraphDataScience classes to disable displaying progress bars when running procedures.

## Bug fixes


## Improvements

* The database connection is now validated before a session is created.
* Retry authentication requests.

## Other changes
