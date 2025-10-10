# Changes in 1.18

## Breaking changes

- Drop support for Python 3.9

## New features

## Bug fixes

- Fixed a bug where `GraphDataScience` object created via `GraphDataScience.from_neo4j_driver` would also close the Neo4j driver object on `close()`. As sole ownership of the driver cannot be expected in this case, the user should close the driver object on their own.

## Improvements

- Added retries to verifying connection and authentication of DB connection provided to `GdsSessions.getOrCreate`


## Other changes

- Add support for PyArrow 21.0.0
- Drop support for PyArrow 17.0
- Support numpy 1.24.0
- Add support for neo4j 6.0
