"""
The integration tests will actually read/write data to the GCP services, so make
sure you are clearly know which project and environment you are using. You also
need to clean up all resources on the GCP platform after the test cases are run.
"""
import os

import pytest
from google.cloud import bigquery

from gfluent import BQ
from gfluent import GCS


PROJECT_ID = "TBD"
BUCKET = "TBD"
PREFIX = "TBD"

# Test Design:
#
# - Create 2 jl files, each has 4 rows
# - Upload them to a remote GCS bucket
# - Create a new dataset for test purpose
# - Call `BQ` load job to load jl files to first table
# - Check returned count should be 4
# - Call `is_exist()` to verify
# - Query the table with age > 20, should return 1 row
# - delete row with age = 15
# - Query the table, should return 3 row
# - Check if the correct name in the returned object and len() it
# - Use query load to copy 3 rows to a second table
# - Check loaded count == 3 and call `is_exist()` to verify
# - Call Load job for all *.jl file to APPEND to second table
# - Check returned count == 3 and total count == 7
# - Call truncate for the first table
# - Check first table should still there, but total row == 0
# - Call drop for the first table
# - Call `is_exist()` to verify non exists
# - Drop the dataset
# - Delete all files in the remote bucket


@pytest.fixture
def make_two_jl_files(tmp_path):
    """
    Name    Age
    Tom     18
    Sid     19

    Zhong   21
    Jacky   15
    """

    with open(os.path.join(tmp_path, "test_one.jl"), "w") as f:
        f.write(
            """{'name':'Tom', 'age':18}
{'name':'Sid', 'age':19}
"""
        )

    with open(os.path.join(tmp_path, "test_two.jl"), "w") as f:
        f.write(
            """{'name':'Zhong', 'age':21}
{'name':'Jacky', 'age':15}
"""
        )

    with open(os.path.join(tmp_path, "test_excluded.csv"), "w") as f:
        f.write("random\n")

    return tmp_path


@pytest.mark.integtest
def test_all_steps(make_two_jl_files):
    dataset_name = "gfluent_int_test"
    table1 = "first_table"
    table2 = "second_table"
    gcs_path = f"gs://{BUCKET}/{PREFIX}"
    uri = f"{gcs_path}/*.jl"

    # upload only *.jl files
    (
        GCS(PROJECT_ID)
        .local(path=make_two_jl_files, suffix=".jl")
        .bucket(BUCKET)
        .prefix(PREFIX)
        .upload()
    )

    # create the testing dataset
    BQ(PROJECT_ID).delete_dataset(dataset_name)
    BQ(PROJECT_ID).create_dataset(dataset_name)

    # call load job to load to first table
    count = BQ(PROJECT_ID).table(f"{dataset_name}.{table1}").gcs(uri).load()

    # loaded count should be 4
    assert count == 4

    # table should exist
    assert BQ(PROJECT_ID).table(f"{dataset_name}.{table1}").is_exist()

    # query the table by select and where
    rows = (
        BQ(PROJECT_ID)
        .sql(f"SELECT * FROM {dataset_name}.{table1} WHERE age > 20")
        .query()
    )
    names = []
    for row in rows:
        names.append(row.name)

    assert len(names) == 1
    assert "Zhong" in names

    # delete the rows with age 15
    rows = (
        BQ(PROJECT_ID)
        .sql(f"DELETE FROM {dataset_name}.{table1} WHERE age = 15")
        .query()
    )
    assert isinstance(rows, bigquery.table._EmptyRowIterator)

    # check all rows, should be 3
    rows = BQ(PROJECT_ID).sql(f"SELECT * FROM {dataset_name}.{table1}").query()
    names = []
    for row in rows:
        names.append(row.name)

    assert len(names) == 3
    assert "Zhong" in names

    # query load to another table
    count = (
        BQ(PROJECT_ID)
        .sql(f"SELECT * FROM {dataset_name}.{table1} WHERE age < 20")
        .table(f"{dataset_name}.{table2}")
        .query()
    )

    assert count == 2
    assert BQ(PROJECT_ID).table(f"{dataset_name}.{table2}").is_exist()

    # APPEND mode to second table
    loaded_count = BQ(PROJECT_ID).table(f"{dataset_name}.{table2}").gcs(uri).load()

    # check loaded rows, should equal what we have in all *.jl files
    assert loaded_count == 4

    # the total count should be 7
    rows = BQ(PROJECT_ID).sql(f"SELECT * FROM {dataset_name}.{table2}").query()
    names = []
    for row in rows:
        names.append(row.name)
    assert len(names) == 6

    # TRUNCATE the first table
    BQ(PROJECT_ID).table(f"{dataset_name}.{table1}").truncate()

    # table should still there, but no rows
    assert BQ(PROJECT_ID).table(f"{dataset_name}.{table1}").is_exist()

    # verify no rows
    rows = BQ(PROJECT_ID).sql(f"SELECT * FROM {dataset_name}.{table1}").query()
    names = []
    for row in rows:
        names.append(row.name)
    assert len(names) == 0

    # Drop the first table
    BQ(PROJECT_ID).table(f"{dataset_name}.{table1}").drop()

    # verify not exists
    assert not BQ(PROJECT_ID).table(f"{dataset_name}.{table1}").is_exist()

    # clean up
    GCS(PROJECT_ID).bucket(BUCKET).prefix(PREFIX).delete()
    BQ(PROJECT_ID).delete_dataset(dataset_name)
