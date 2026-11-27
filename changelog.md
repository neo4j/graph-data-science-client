# Changes in 2.5

## Breaking changes


## New features


- `DbmsConnectionInfo` accepts an already connected `neo4j.Driver` via the new `driver` parameter,
  which can be passed to `GdsSessions.get_or_create`. The driver is not closed by the session.
  `uri` and `aura_instance_id` are optional in this case.

## Bug fixes


## Improvements


## Other changes
