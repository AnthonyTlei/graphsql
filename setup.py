from setuptools import setup, find_packages

setup(
    name="graphsql",
    version="0.1.0",
    author="Anthony Tleiji",
    author_email="anthonytleiji@gmail.com",
    description="GraphQL SQLAlchemy dialect for Superset and other applications.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/AnthonyTlei/graphsql",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    package_data={
        "graphsql": ["introspection/*.graphql"]
    },
    install_requires=[
        "sqlalchemy<2.0",
        "pandas",
        "pyarrow",
        "requests",
        "sqlparse",
        "duckdb"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    entry_points={
        "sqlalchemy.dialects": [
            "graphsql = graphsql.dialect:GraphSQLDialect"
        ]
    },
)