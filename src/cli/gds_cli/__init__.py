"""The ``gds`` command-line interface: ``gds run ...`` and ``gds database ...``.

This subpackage is only imported when the ``gds`` console script runs (see
``[project.scripts]`` in ``pyproject.toml``); ``import graphdatascience`` never
imports it, so its extra dependencies (``typer``, ``rich``, ``pyyaml``,
``jsonschema``) are only required when the ``cli`` extra is installed.
"""
