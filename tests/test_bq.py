from unittest.mock import ANY
from unittest.mock import call
from unittest.mock import patch

import pytest
from google.cloud import bigquery

from gfluent import BQ


@pytest.fixture
def bq_client():
    project = "here-is-project-id"

    schema = [
        bigquery.SchemaField("exec_id", "INTEGER", "desc"),
        bigquery.SchemaField("name", "STRING", "desc"),
    ]
    with patch("gfluent.bq.bigquery.Client", autospec=True):
        yield BQ(
            project,
            table="dataset.table",
            gcs="gs://abc",
            sql="select *",
            schema=schema,
            mode="WRITE_APPEND",
            create_mode="CREATE_NEVER",
            ignore="ignored",
            format="CSV",
        )


def test_init_with_project_id():
    project = "here-is-project-id"
    with patch("gfluent.bq.bigquery.Client", autospec=True):
        bq = BQ(project)

    assert bq._project == project
    assert bq._mode == "WRITE_APPEND"
    assert bq._create_mode == "CREATE_IF_NEEDED"
    assert bq._format == "NEWLINE_DELIMITED_JSON"


def test_init_with_kwargs(bq_client):
    assert bq_client._table == "dataset.table"
    assert bq_client._gcs == "gs://abc"
    assert bq_client._sql == "select *"
    assert bq_client._mode == "WRITE_APPEND"
    assert bq_client._create_mode == "CREATE_NEVER"
    assert bq_client._format == "CSV"

    with pytest.raises(AttributeError):
        bq_client.ignore


def test_repr(bq_client):
    assert str(bq_client) == "BQ(project=here-is-project-id)"


def test_export_not_implemented_error(bq_client):
    with pytest.raises(NotImplementedError):
        bq_client.export()


def test_change_attribute_after_init(bq_client):
    bq_client.table("new.table")
    assert bq_client._table == "new.table"

    bq_client.gcs("gs://new")
    assert bq_client._gcs == "gs://new"

    bq_client.sql("with new")
    assert bq_client._sql == "with new"

    bq_client.sql("delete from some table")
    assert bq_client._sql == "delete from some table"

    bq_client.mode("WRITE_TRUNCATE")
    assert bq_client._mode == "WRITE_TRUNCATE"

    bq_client.create_mode("CREATE_NEVER")
    assert bq_client._create_mode == "CREATE_NEVER"

    bq_client.format("NEWLINE_DELIMITED_JSON")
    assert bq_client._format == "NEWLINE_DELIMITED_JSON"


def test_invalid_gcs(bq_client):
    with pytest.raises(ValueError):
        bq_client.gcs("not_a_gcs_prefix")


def test_invalid_sql(bq_client):
    with pytest.raises(ValueError):
        bq_client.sql("insert into")


def test_invalid_schema(bq_client):
    with pytest.raises(TypeError):
        bq_client.schema("not a list")


def test_invalid_mode(bq_client):
    with pytest.raises(ValueError):
        bq_client.mode("wrong mode")


def test_invalid_create_mode(bq_client):
    with pytest.raises(ValueError):
        bq_client.create_mode("wrong mode")


def test_invalid_format(bq_client):
    with pytest.raises(ValueError):
        bq_client.format("wrong format")


def test_invalid_table_name(bq_client):
    with pytest.raises(ValueError):
        bq_client.table("no_dot_in_table")


def test_invalid_init():
    with patch("gfluent.bq.bigquery.Client", autospec=True):
        with pytest.raises(ValueError):
            _ = BQ(project="proj_id", mode="wrong mode")

        with pytest.raises(ValueError):
            _ = BQ(project="proj_id", gcs="not_a_gcs_prefix")

        with pytest.raises(ValueError):
            _ = BQ(project="proj_id", sql="insert into abc")

        with pytest.raises(TypeError):
            _ = BQ(project="proj_id", schema="not a list")

        with pytest.raises(ValueError):
            _ = BQ(project="proj_id", mode="wrong mode")

        with pytest.raises(ValueError):
            _ = BQ(project="proj_id", create_mode="wrong mode")

        with pytest.raises(ValueError):
            _ = BQ(project="proj_id", format="wrong mode")

        with pytest.raises(ValueError):
            _ = BQ(project="proj_id", table="wrong table name")


@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_query_is_called(obj):
    bq = BQ("project_id")
    bq.sql("select * from table")
    bq.query()

    obj.assert_called_once_with(project="project_id")
    obj.return_value.query.assert_called_once_with("select * from table")


@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_query_is_called_and_returned(obj):
    bq = BQ("project_id")
    bq.sql("select * from table")
    obj.return_value.query.return_value.result.return_value = "data"
    assert bq.query() == "data"


@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_query_load_is_called(obj):
    bq = BQ("project_id")
    bq.sql("select * from table")
    bq.table("my.table")
    bq.query()

    obj.assert_called_once_with(project="project_id")
    obj.return_value.query.assert_called_once_with(
        "select * from table", job_config=ANY
    )


@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_query_load_is_called_total_rows_returned(obj):
    """For a `queryJob`, the return row count is actually what has been queried,"""
    bq = BQ("project_id")
    bq.sql("select * from table")
    bq.table("my.table")
    obj.return_value.query.return_value.result.return_value.total_rows = 100
    assert bq.query() == 100


@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_load_table_from_uri_is_called(obj):
    bq = BQ("project_id")
    bq.table("my.table")
    bq.gcs("gs://gcslocation/")
    bq.load()

    obj.assert_called_once_with(project="project_id")
    obj.return_value.load_table_from_uri.assert_called_once_with(
        "gs://gcslocation/", "project_id.my.table", location="US", job_config=ANY
    )


@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_load_table_get_table_called_twice(obj):
    """For the WRITE_APPEND mode (default), it should check the count twice and calculate
    the different. As we mocked the return value of num_rows = 100, the final return
    should be 0, as 100 - 100 = 0
    """
    bq = BQ("project_id")
    bq.table("my.table")
    bq.gcs("gs://gcslocation/")

    # make sure the is_exist() return True
    obj.return_value.get_table.return_value.num_rows = 100

    assert bq.load() == 0

    table_id = "project_id.my.table"

    get_table_calls = [call(table_id), call(table_id)]

    obj.return_value.get_table.assert_has_calls(get_table_calls, any_order=True)


@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_load_table_get_table_called_once(obj):
    """For the WRITE_EMPTY mode, it should call `get_table` only once and using
    the 0 as `count_before_load`, so the final return should be 100, as 100 - 0 = 100
    """
    bq = BQ("project_id")
    bq.table("my.table")
    bq.gcs("gs://gcslocation/")
    bq.mode("WRITE_EMPTY")

    # # make sure the is_exist() return True
    obj.return_value.get_table.return_value.num_rows = 100

    assert bq.load() == 100

    table_id = "project_id.my.table"

    obj.return_value.get_table.assert_called_once_with(table_id)


@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_truncate_is_called(obj):
    bq = BQ("project_id")
    bq.table("my.table")
    bq.truncate()

    obj.assert_called_once_with(project="project_id")
    obj.return_value.query.assert_called_once_with("TRUNCATE TABLE project_id.my.table")


@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_create_table_is_called(obj):
    schema = [
        bigquery.SchemaField(
            name="name",
            field_type="STRING",
            mode="REQUIRED",
            description="student name",
        ),
        bigquery.SchemaField(
            name="age", field_type="INTEGER", mode="REQUIRED", description="student age"
        ),
    ]
    bq = BQ("project_id").table("my.table").schema(schema)
    bq.create()

    obj.assert_called_once_with(project="project_id")
    obj.return_value.create_table.assert_called_once()


@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_drop_table_is_called_using_drop(obj):
    bq = BQ("project_id").table("my.table")
    bq.drop()

    obj.assert_called_once_with(project="project_id")
    obj.return_value.delete_table.assert_called_once_with(
        "project_id.my.table", not_found_ok=True
    )


@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_drop_table_is_called_using_delete(obj):
    bq = BQ("project_id").table("my.table")
    bq.delete()

    obj.assert_called_once_with(project="project_id")
    obj.return_value.delete_table.assert_called_once_with(
        "project_id.my.table", not_found_ok=True
    )


@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_create_dataset_is_called(obj):
    bq = BQ("project_id")
    bq.create_dataset("ds_name")

    obj.assert_called_once_with(project="project_id")
    obj.return_value.create_dataset.assert_called()


@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_delete_dataset_is_called(obj):
    bq = BQ("project_id")
    bq.delete_dataset("ds_name")

    obj.assert_called_once_with(project="project_id")
    obj.return_value.delete_dataset.assert_called_with(
        "project_id.ds_name", delete_contents=True, not_found_ok=True
    )


@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_get_table_is_called(obj):
    bq = BQ("project_id").table("my.table")
    _ = bq.is_exist()

    obj.assert_called_once_with(project="project_id")
    obj.return_value.get_table.assert_called_with("project_id.my.table")
