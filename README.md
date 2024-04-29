# IEI Data API

A pythonic interface for interacting with the IEI data warehouse.

## Installation

Create an env and then install this package into your env.

```bash
pip install git+ssh://git@github.com:internet-equity/iei_data_api.git
```

Or you can clone the repo and install it directly (add the `-e` flag to install in dev mode, where changes to your source code is immediately available in your env).

```python
pip install -e path/to/your-iei_data_api-clone
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
