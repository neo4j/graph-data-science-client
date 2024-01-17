# Changes in 1.9


## Breaking changes


## New features

* Add the new concept of GDS Sessions, used to manage GDS computations in Aura, based on data from an AuraDB instance.
  * Add a new `gds.graph.project` endpoint to project graphs from AuraDB instances to GDS sessions.


## Bug fixes

* Fixed a bug which caused an exception to be raised when running `gds.license.state` targeting an AuraDS instance.
* Fixed a bug where the parameter `listNodeLabels` was ignored for `gds.graph.[nodeProperty|nodeProperties].stream` calls via Arrow.
* Fixed a bug where the parameter `listNodeLabels` was ignored for `gds.graph.[nodeProperty|nodeProperties].stream` calls via Cypher and `separate_property_columns=True`.


## Improvements

* Expose user facing custom types so that they can be directly imported from `graphdatascience`.
* Allow dropping graphs through `gds.graph.drop` by name and not only based on `Graph` objects.


## Other changes
