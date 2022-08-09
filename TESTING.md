# graphdatascience testing

This document describes how to run tests for `graphdatascience` as well as how to check that the code style is maintained.

Tests can be found in `graphdatascience/tests`. In each of the folders there, `unit` and `integration`, the tests of that respective type reside.

Please see the section [Specifically for this project](CONTRIBUTING.md#specifically-for-this-project) of our [contribution guidelines](CONTRIBUTING.md) for how to set up an environment for testing and style checking.


## Unit testing

The unit tests are run without a connection a database. Typically the `CollectingQueryRunner` is used to mock running queries.

To run the unit tests (with default options), simply call:

```bash
pytest graphdatascience/tests/unit
```


## Integration testing

In order to run the integration tests one must have a [Neo4j DBMS](https://neo4j.com/docs/getting-started/current/) with the Neo4j Graph Data Science library installed running.


### Configuring

The tests will through the [Neo4j Python driver](https://neo4j.com/docs/python-manual/current/) connect to a Neo4j database based on the environment variables:

* `NEO4J_URI` (defaulting to "bolt://localhost:7687" if unset),
* `NEO4J_USER`,
* `NEO4J_PASSWORD` (defaulting to "neo4j" if unset),
* `NEO4J_DB` (defaulting to "neo4j" if unset).

However, if `NEO4J_USER` is not set the tests will try to connect without authentication.

Once the driver connects successfully to the Neo4j DBMS the tests will go on to execute against the `NEO4J_DB` database.


### Running

To run the integration tests (with default options), simply call:

```bash
pytest graphdatascience/tests/integration
```

To include tests that require the Enterprise Edition of the Neo4j Graph Data Science library, you must specify the option `--include-enterprise`.
Naturally, this requires access to a valid Neo4j GDS license key, which can be acquired via the [Neo4j GDS product website](https://neo4j.com/product/graph-data-science/).

To include tests that exercise storing and loading models, you must specify the option `--include-model-store-location`.
Note however that this also requires you to have specified a valid path for the `gds.model.store_location` configuration key of your database.


### Running tests that require encrypted connections

In order to run integration tests that test encryption features, you must setup the Neo4j server accordingly:

```
# Enable the Arrow Flight Server (necessary if you run integration tests that require Arrow)
gds.arrow.enabled=true

# Allow bolt connections either encrypted or unencrypted
dbms.connector.bolt.tls_level=OPTIONAL
dbms.ssl.policy.bolt.enabled=true
dbms.ssl.policy.bolt.base_directory=<absolute-path-to-graph-datascience-client>/graphdatascience/tests/integration/resources
dbms.ssl.policy.bolt.public_certificate=arrow-flight-gds-test.crt
dbms.ssl.policy.bolt.private_key=arrow-flight-gds-test.key
dbms.ssl.policy.bolt.client_auth=NONE
```

To run only integration tests that are marked as `encrypted_only`, call:

```bash
pytest graphdatascience/tests/integration --encrypted-only
````


### GDS library versions

There are integration tests that are only compatible with certain versions of the GDS library.
For example, a procedure (which does not follow the standard algorithm procedure pattern) introduced in version 2.1.0 of the library will not exist in version 2.0.3, and so any client side integration tests that call this procedure should not run when testing against server library version 2.0.3.
For this reason only tests compatible with the GDS library server version you are running against will run.


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

from the root. See the `[tool.black]` section of `pyproject.toml` for our custom black settings.

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


