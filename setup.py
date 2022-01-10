import setuptools

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

with open("requirements/base.txt", "r", encoding="utf-8") as f:
    reqs = f.read().split()

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
    "Documentation": "https://neo4j.com/docs/graph-data-science/current/",
    "Source": "https://github.com/neo4j/gdsclient",
    "Bug Tracker": "https://github.com/neo4j/gdsclient/issues",
}

setuptools.setup(
    name="gdsclient",
    version="0.0.4",
    author="Neo4j",
    author_email="team-gds@neo4j.org",
    description="Python bindings for the Neo4j Graph Data Science library",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="Apache License 2.0",
    url="https://neo4j.com/product/graph-data-science/",
    classifiers=classifiers,
    packages=setuptools.find_packages(),
    package_data={"gdsclient": ["py.typed"]},
    project_urls=project_urls,
    python_requires=">=3.6",
    install_requires=reqs,
    zip_safe=False,
)
