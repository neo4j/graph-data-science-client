# Changes in 1.7


## Breaking changes


## New features

* Add a new method `GraphDataScience.server_version` which returns the version of the server not as a `str` but as a `ServerVersion`. This allows easier inspection of the major, minor and patch version.


## Bug fixes


## Improvements

* When an almost correct method is called, raise an error with a message that suggests the most probable correct method name that was intended.
* Improved IDE auto-completion support to give significantly fewer false positive suggestions.
* Failing to log progress of a call will no longer fail the call itself, but just warn that logging was unsuccessful.
* Underlying connections to a Neo4j DBMS is now being verified and retried automatically up to a timeout of 10 minutes.


## Other changes
