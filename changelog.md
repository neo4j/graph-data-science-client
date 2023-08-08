# Changes in 1.8


## Breaking changes


## New features

* New method `gds.graph.cypher.project` to project a graph using Cypher projection.
  More details can be found in the user guide.


## Bug fixes

* Fixed a bug, where the graph object would list multiple graphs if the same name was used for graphs on different databases.


## Improvements

* Improved endpoint returning graphs to be used in `with` clauses. The expression `with gds.graph.project(...)[0] as G` can now be simplified to `with gds.graph.project(...) AS G`.
* Improve the error message if GDS is not correctly installed on the server


## Other changes
* Dropped Python 3.7 support which is EOL.


