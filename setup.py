import setuptools

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

with open("requirements/base.txt", "r", encoding="utf-8") as f:
    reqs = f.read().split()

setuptools.setup(
    name="gdsclient",
    version="0.0.1",
    author="Neo4j",
    author_email="team-gds@neo4j.org",
    description="Python bindings for the Neo4j Graph Data Science library",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="Apache License 2.0",
    url="https://github.com/neo4j/graph-data-science",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(),
    package_data={"gdsclient": ["py.typed"]},
    python_requires=">=3.6",
    install_requires=reqs,
    zip_safe=False,
)
