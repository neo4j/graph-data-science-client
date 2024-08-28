# Changes in 1.11


## Breaking changes


## New features

* Add Neo4j python driver rust extension as a new optional dependency.
* Support creating GDS Sessions for self-managed Neo4j DBMS.
  * `GdsSessions.get_or_create` requires a new parameter `cloud_location` to specify where the session will be created.
* Add `ttl` parameter to `GdsSessions.get_or_create` to control if and when an unused session will be automatically deleted.

## Bug fixes


## Improvements

* The database connection is now validated before a session is created.

## Other changes
