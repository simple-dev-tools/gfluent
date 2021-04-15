# Google Cloud Fluent Client

This is a wrapper on Google Cloud Platform Python SDK client library. It provides a fluent-style to
call the methods. The idea is, there are too many parameters for Google `Storage` and `BigQuery`,
however, most of them are ok to be set as default value. 

This library is good for Data Engineer to create data pipeline based on `BigQuery`, here is an example
of a end to end user case.

You are asked to

1 - load multiple files from your local drive to GCS
2 - load those files to a BigQuery table
3 - run another query on that table by joining other tables, store the result to another table


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


Here is the [document](https://gfluent.readthedocs.io/en/latest/#), and please refer
to the test cases to see more real examples.

This project is in the inital phase.


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


## Testing

The unit test and integration test are actually using the real GCP project, so you
cannot execute the integration test if you don't have the GCP project setup.

If you really want to run the test cases, you need to set up a free tier project, and
set the project ID as `PROJECT_ID` enviroment, you also need to expose the GCP JSON key
of the service account with correct permission of read/write `BigQuery` and `GCS`.

