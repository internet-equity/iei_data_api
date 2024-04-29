from setuptools import setup, find_packages

setup(
    author="Matt Triano",
    description="A pythonic interface to IEI's data warehouse.",
    name="iei_data_api",
    version="0.1.0",
    packages=["iei_data_api"],
    install_requires=[
        "pandas",
        "psycopg2",
        "sqlalchemy>=2.0",
    ],
)

