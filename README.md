# Google Cloud Fluent Client

This is a wrapper on Google Cloud Platform Python SDK client library. It provides a fluent-style to
call the methods, here is an example,

```python

from gfluent import BQ

project_id = "here-is-you-project-id"
bq = BQ(project_id, table="mydataset.table")

result = bq.mode("WRITE_APPEND").sql("SELECT name, age from dataset.tabble").query()

print(f"The query has inserted {result} rows to table mydataset.table")

```
