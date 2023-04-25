# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
import os
import sys

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Neo4j Graph Data Science Python Client"
copyright = "2023, Neo4j"
author = "Neo4j"
release = "2023"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

# Add the root of the project to the path so that Sphinx can find the Python sources
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

extensions = [
    "sphinx.ext.autodoc",  # include docs from docstrings
    "sphinx.ext.napoleon",  # Support for NumPy and Google style docstrings
]

# napoleon_use_rtype = False

templates_path = ["_templates"]
exclude_patterns = []  # type: ignore


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
html_static_path = ["_static"]
