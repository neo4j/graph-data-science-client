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
version = "1.8"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

# Add the root of the project to the path so that Sphinx can find the Python sources
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

extensions = [
    "sphinx.ext.autodoc",  # include docs from docstrings
    "sphinx.ext.napoleon",  # Support for NumPy and Google style docstrings
]

autodoc_class_signature = "separated"

templates_path = ["_templates"]
exclude_patterns = []  # type: ignore

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# use neo4j theme, which extends neo4j docs css for sphinx

html_theme = "neo4j"
html_theme_path = ["themes"]


# 01-nav.js is a copy of a js file of the same name that is included in the docs-ui bundle
def setup(app):  # type: ignore
    app.add_js_file("https://neo4j.com/docs/assets/js/site.js", loading_method="defer")
    app.add_js_file("js/12-fragment-jumper.js", loading_method="defer")
    app.add_js_file("js/deprecated.js", loading_method="defer")


rst_epilog = """
.. |api-version| replace:: {versionnum}
""".format(
    versionnum=version,
)
