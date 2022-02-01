import setuptools

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

with open("requirements/base.txt", "r", encoding="utf-8") as f:
    reqs = f.read().splitlines()

classifiers = [
    "Intended Audience :: Developers",
    "Intended Audience :: Science/Research",
    "License :: OSI Approved :: Apache Software License",
    "Operating System :: OS Independent",
    "Programming Language :: Python :: 3",
    "Topic :: Database",
    "Topic :: Scientific/Engineering",
    "Topic :: Software Development",
    "Typing :: Typed",
]

project_urls = {
    "Documentation": "https://neo4j.com/docs/graph-data-science/2.0-preview/python-client/",
    "Source": "https://github.com/neo4j/graph-data-science-client",
    "Bug Tracker": "https://github.com/neo4j/graph-data-science-client/issues",
}

setuptools.setup(
    name="graphdatascience",
    version="0.0.9",
    author="Neo4j",
    author_email="team-gds@neo4j.org",
    description="A Python client for the Neo4j Graph Data Science (GDS) library",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="Apache License 2.0",
    url="https://neo4j.com/product/graph-data-science/",
    classifiers=classifiers,
    packages=setuptools.find_packages(),
    package_data={"graphdatascience": ["py.typed"]},
    project_urls=project_urls,
    python_requires=">=3.6",
    install_requires=reqs,
    zip_safe=False,
)
