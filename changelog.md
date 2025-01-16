# Changes in 1.14


## Breaking changes


## New features


## Bug fixes

* Fixed a bug, where sessions could not be created for AuraDB instances of tier `business-critical`.
* Fixed a bug, where sessions would fail on write-back if the Graph was empty.


## Improvements

* Display progress bar for remote projection and open-ended tasks.
* Allow passing the optional graph filter also as type `str` to `gds.graph.list()` instead of only `Graph`.


## Other changes
