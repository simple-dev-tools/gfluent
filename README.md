# Google Cloud Fluent Client

[![UT & SIT](https://github.com/simple-dev-tools/gfluent/actions/workflows/ut-and-sit.yml/badge.svg?branch=develop)](https://github.com/simple-dev-tools/gfluent/actions/workflows/ut-and-sit.yml)

This is a lightweight wrapper on top of Google Cloud Platform Python SDK client library. It provides
a fluent-style to call the methods. The motivation is, too many parameters for GCP `Storage` and
`BigQuery` library, and most of them are ok to be set as default values. 

This wrapper is suitable for Data Engineers to quickly create simple data pipeline based on GCP
`BigQuery` and `Storage`, here are two examples.


## Build Data Pipeline on BigQuery

You (A Data Engineer) are asked to,

- load multiple `json` files from your local drive to GCS

- import those files to a BigQuery staging table

- run another query based on the staging table by joining existing tables, and store the result to another table


To accomplish the task, here are the source code,

```python

from gfluent import BQ, GCS

project_id = "here-is-you-project-id"
bucket_name = "my-bucket"
dataset = "sales"
table_name = "products"
prefix = "import"
local_path = "/user/tom/products/" # there are many *.json files in this directory

# uplaod files to GCS bucket
(
    GCS(project_id)
    .local(path=local_path, suffix=".json" )
    .bucket(bucket_name)
    .prefix(prefix)
    .upload()
)

# if you need to create the dataset
BQ(project_id).create_dataset(dataset, location="US")

# load data to BigQuery table

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


# run a query

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

print(f"{number_of_rows} rows are appended")


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

```python
import os
from gfluent import Sheet, BQ

project_id = 'your project id'
sheet_id = 'your Google sheet id`

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
pip install -r requirements-dev.txt
poetry build
pip install dist/gfluent-<versoin>.tar.gz
```


## Contribution

Any kinds of contribution is welcome, including report bugs, add feature or enhuance document. Please
be noted, the Integration Test is using a real GCP project, and you may not have the permission to
set up the test data.
