from setuptools import setup, find_packages

with open("README.md", "r") as f:
    full_readme = f.read()

setup(
    author="Matt Triano",
    name="iei_data_api",
    version="0.1.0",
    description="A pythonic interface to IEI's data warehouse.",
    long_description=full_readme,
    long_description_content_type="text/markdown",
    packages=["iei_data_api"],
    install_requires=[
        "psycopg2-binary",
        "sqlalchemy>=2.0",
        "geopandas>=1.0",
        "GeoAlchemy2",
    ],
)

