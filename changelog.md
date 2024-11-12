# Changes in 1.13


## Breaking changes


## New features

* Add `GdsSessions::available_cloud_locations()` to return available locations for session creation.


## Bug fixes


## Improvements

* Inform about Session failures such as `Out of Memory`. `GdsSession::get_or_create` will fail and `GdsSessions::list_sessions` will return the errors in a new field along `errors`.


## Other changes
