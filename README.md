# IEI Data API

A pythonic interface for interacting with the IEI data warehouse.

## Installation

Create an env and then install this package into your env.

```python
pip install git+ssh://git@github.com:internet-equity/iei_data_api
```

## Usage

First, create a .env.dwh file and define the following environment variables.

```bash
POSTGRES_USER="dwh_db_user"
POSTGRES_PASSWORD="dwh_db_pass"
POSTGRES_DB="dwh_db_name"
```

Then you can import it and (assuming you're running from a context on Abbott), access the database.

```python
from iei_data_api.catalog import DataCatalog

catalog = DataCatalog(Path("path/to/.env.dwh"))

results_df = catalog.query("SELECT * FROM a_schema.a_table")
```
