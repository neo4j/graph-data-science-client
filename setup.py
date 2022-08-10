import setuptools

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

with open("requirements/base.txt", "r", encoding="utf-8") as f:
    reqs = f.read().splitlines()

with open("graphdatascience/version.py") as f:
    version = f.readline().strip().split()[-1][1:-1]

classifiers = [
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "Intended Audience :: Science/Research",
    "License :: OSI Approved :: Apache Software License",
    "Operating System :: OS Independent",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3 :: Only",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Topic :: Database",
    "Topic :: Scientific/Engineering",
    "Topic :: Software Development",
    "Typing :: Typed",
]

project_urls = {
    "Documentation": "https://neo4j.com/docs/graph-data-science/current/python-client/",
    "Source": "https://github.com/neo4j/graph-data-science-client",
    "Bug Tracker": "https://github.com/neo4j/graph-data-science-client/issues",
}

setuptools.setup(
    name="graphdatascience",
    version=version,
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
    python_requires=">=3.7",
    install_requires=reqs,
    zip_safe=False,
)
