# Changes in 1.13


## Breaking changes

* Drop support for Python `3.8` as its EOL
* Drop support for PyArrow 14


## New features

* Add `GdsSessions::available_cloud_locations()` to return available locations for session creation.


## Bug fixes


## Improvements

* Inform about Session failures such as `Out of Memory`. `GdsSession::get_or_create` will fail and `GdsSessions::list_sessions` will return the errors in a new field along `errors`.
* Improve error message around connection issues against Sessions.
* Avoid a query getting stuck on fetching progress logging if the database is unavailable.


## Other changes

* Added support for PyArrow 18
