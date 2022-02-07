# Google Cloud Fluent Client

[![Unit Testing](https://github.com/simple-dev-tools/gfluent/actions/workflows/ut.yml/badge.svg)](https://github.com/simple-dev-tools/gfluent/actions/workflows/ut.yml)
[![Deployment](https://github.com/simple-dev-tools/gfluent/actions/workflows/deployment.yml/badge.svg)](https://github.com/simple-dev-tools/gfluent/actions/workflows/deployment.yml)
[![PyPI version](https://badge.fury.io/py/gfluent.svg)](https://badge.fury.io/py/gfluent)

*Version: 1.2.0*

This is a lightweight wrapper on top of Google Cloud Platform Python SDK client libraries `BigQuery`,
`Storage` and `Spreadsheet`. It is a great package for Data Engineers for craft data pipeline by using
`BigQuery` and `Storage` as major services from Google Cloud Platform.

The purpose of this package are,

- Having a consistent way of using the GCP client libraries
- Manage the version in a single place if multiple teams are using the GCP client libraries
- Make it easier to accomplish the typical Data Engineering tasks (copy data, load and export)
- The code explains what it does


The current embedded client libraires versions are,

- google-api-python-client==2.36.0
- google-cloud-bigquery==2.32.0
- google-cloud-storage==2.1.0

## Build Data Pipeline on BigQuery

You (A Data Engineer) are asked to,

- Upload multiple `json` files from your local drive to GCS
- Import those files to a BigQuery staging table
- Run a SQL query based on the staging table by joining existing tables, and store the result
to a new table


To accomplish the task, here are the source code,

```python

from gfluent import BQ, GCS

project_id = "here-is-you-project-id"
bucket_name = "my-bucket"
dataset = "sales"
table_name = "products"
prefix = "import"
local_path = "/user/tom/products/" # there are few *.json files in this directory

# uplaod files to GCS bucket
(
    GCS(project_id)
    .local(path=local_path, suffix=".json" )
    .bucket(bucket_name)
    .prefix(prefix)
    .upload()
)

# create the target dataset (in case not exists)
BQ(project_id).create_dataset(dataset, location="US")

# load json files to BigQuery table
uri = f"gs://{bucket_name}/{prefix}/*.json"
number_of_rows = (
    BQ(project_id)
    .table(f"{dataset}.{table_name}")
    .mode("WRITE_APPEND")               # don't have to, default mode
    .create_mode("CREATE_IF_NEEDED")    # don't have to, default mode
    .format("NEWLINE_DELIMITED_JSON")   # don't have to, default format
    .gcs(uri).load(location="US")
)

print(f"{number_of_rows} rows are loaded")


# run a SQL query and save to a final table
final_table = "sales_summary"
sql = """
    select t1.col1, t2.col2, t2.col3
    FROM
        sales.products t1
    JOIN
        other.category t2
    ON  t1.prod_id = t2.prod_id
"""

number_of_rows = (
    BQ(product_id)
    .table(f"{dataset}.{final_table}")
    .sql(sql)
    .create_mode("CREATE_NEVER")    # have to, don't want to create new table
    .query()
)

print(f"{number_of_rows} rows are loaded to {final_table}")


# now let's query the new table
rows = (
    BQ(product_id)
    .sql(f"select col1, col2 from {dataset}.{final_table} limit 10")
    .query()
)

for row in rows:
    print(row.col1, row.col2)
```

## Loading data from Spreadsheet to BigQuery

Here is another example to use the `Sheet` class for loading data from Google Spreadsheet.

```python
import os
from gfluent import Sheet, BQ

project_id = 'your-project-id'
sheet_id = 'the-google-spreadsheet-id'

# assume the data is on the sheet `data` and range is `A1:B4`
sheet = Sheet(
    os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
).sheet_id(sheet_id).worksheet("data!A1:B4")

bq = BQ(project=project_id).table("target_dataset.table")

sheet.bq(bq).load(location="EU")
```

## Documents

Here is the [document](https://gfluent.readthedocs.io/en/latest/#), and please refer to the test
cases to see more real examples.


## Installation

Install from PyPi,

```bash
pip install -U gfluent
```

Or build and install from source code,

```bash
git clone git@github.com:simple-dev-tools/gfluent.git
cd gfluent
make test-ut
python setup.py install
```


## Contribution

Any kinds of contribution is welcome, including report bugs, add feature or enhance the document. Please
be noted,

- Unit Testing with mock is intensively used, because we don't want to connect to a real GCP project
- Please install `pre-commit` by using `pip install pre-commit` then `pre-commit install`
- `bump2version` is used for update the version tag in various files
