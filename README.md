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


```python

from gfluent import GCS

project_id = "here-is-you-project-id"

# upload single local `file.txt` to `gs://bucket-name/import/file.txt`
GCS(project_id).bucket("bucket-name").local("/tmp/file.txt").prefix("import").upload()

# upload many local files to GCS
# if you have /tmp/abc.txt, /tmp/111.txt, /tmp/abc.csv
# two GCS objects will be created
# gs://bucket-name/import/abc.txt
# gs://bucket-name/import/111.txt
(
    GCS(project_id)
    .bucket("bucket-name")
    .local(path="/tmp", suffix=".txt").prefix("import").upload()
)

```