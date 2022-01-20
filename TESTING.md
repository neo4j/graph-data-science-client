# graphdatascience testing

This document describes how to run tests for `graphdatascience` as well as how to check that the code style is maintained.

Tests can be found in `graphdatascience/tests`. In each of the folders there, `unit` and `integration`, the tests of that respective type reside.

To install all Python requirements for testing and code style checking, simply run:

```bash
pip install -r requirements/dev.txt
```


## Unit testing

The unit tests are run without a connection a database. Typically the `CollectingQueryRunner` is used to mock running queries.
To run the unit tests, simply call:

```bash
pytest graphdatascience/tests/unit
```


## Integration testing

In order to run the integration tests one must have a [Neo4j DBMS](https://neo4j.com/docs/getting-started/current/) with the Neo4j Graph Data Science library installed running.

The tests will through the [Neo4j Python driver](https://neo4j.com/docs/python-manual/current/) connect to a Neo4j DBMS based on the environment variables:

* `NEO4J_URI` (defaulting to "bolt://localhost:7687" if unset),
* `NEO4J_USER`,
* `NEO4J_PASSWORD` (defaulting to "neo4j" if unset).

However, if `NEO4J_USER` is not set the tests will try to connect without authentication.

Once the driver connects successfully to the Neo4j DBMS the tests will go on to execute against the DBMS's default database.

The command for running the integration tests is the following:

```bash
pytest graphdatascience/tests/integration
```

To include tests that require the Enterprise Edition of the Neo4j Graph Data Science library, you can specify the option `--include-enterprise`.
Naturally, this requires access to a valid Neo4j GDS license key, which can be acquired via the [Neo4j GDS product website](https://neo4j.com/product/graph-data-science/).


## Style guide

The code follows a rather opinionated style based on [pep8](https://www.python.org/dev/peps/pep-0008/).
You can check all code using all the below mentioned code checking tools by running the `scripts/checkstyle` bash script.
There's also a `scripts/makestyle` to do formatting.


### Linting

To enforce pep8 conformity (with the exception of using max line length = 120) [flake8](https://flake8.pycqa.org/en/latest/) is used.
To run it to check the entire repository, simply call:

```bash
flake8
```

from the root. See `.flake8` for our custom flake8 settings.


### Formatting

For general formatting we use [black](https://black.readthedocs.io/en/stable/) with default settings.
black can be run to format the entire repository by calling:

```bash
black .
```

from the root.

Additionally [isort](https://pycqa.github.io/isort/) is used for consistent import sorting.
It can similarly be run to format all source code by calling:

```bash
isort .
```

from the root. See `.isort.cfg` for our custom isort settings.


### Static typing

The code is annotated with type hints in order to provide documentation and allow for static type analysis with [mypy](http://mypy-lang.org/).
Please note that the `typing` library is used for annotation types in order to stay compatible with Python versions < 3.9.
To run static analysis on the entire repository with mypy, just run:

```bash
mypy .
```

from the root. See `mypy.ini` for our custom mypy settings.


