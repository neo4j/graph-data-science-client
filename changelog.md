# Changes in 1.8


## Breaking changes


## New features


## Bug fixes


## Improvements

* Improved endpoint returning graphs to be used in `with` clauses. The expression `with gds.graph.project(...)[0] as G` can now be simplified to `with gds.graph.project(...) AS G`.


## Other changes
* Dropped Python 3.7 support which is EOL.


