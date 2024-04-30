# IEI Data API

A pythonic interface for interacting with the IEI data warehouse.

## Installation

Create an env and then install this package into your env.

```bash
python -m pip install git+ssh://git@github.com/internet-equity/iei_data_api.git
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
POSTGRES_DB="iei_dwh"
```

Then you can import it and (assuming you're running from a context on Abbott), access the database.

```python
from pathlib import Path
from iei_data_api.catalog import DataCatalog

catalog = DataCatalog(Path("path/to/.env.dwh"))

results_df = catalog.query("SELECT * FROM a_schema.a_table")
```

I've also included some database inspection functionality, demonstrated below.

```python
catalog.get_schema_names()
['information_schema',
 'netrics_clean',
 'netrics_raw',
 'public',
 'public_netrics_clean',
 'public_netrics_raw',
 'tiger',
 'tiger_data',
 'topology']

catalog.get_table_names(schema="netrics_raw")
['ip_raw',
 'lml_raw',
 'speed_ookla_raw',
 'speed_ndt7_raw',
 'dev_raw',
 'hops_raw',
 'dns_latency_raw',
 'ping_raw']
```

