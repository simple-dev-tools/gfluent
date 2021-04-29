import pytest
from os import path

from google.cloud import bigquery

from gfluent import BQ


class TestBQ():
    def setup_class(self):
        project = "here-is-project-id"

        self.schema = [
            bigquery.SchemaField(
                "exec_id", "INTEGER", "desc"
            ),
            bigquery.SchemaField(
                "name", "STRING", "desc"
            ),
        ]

        self.bq = BQ(project, 
            table="dataset.table", 
            gcs="gs://abc",
            sql="select *", 
            schema=self.schema,
            mode="WRITE_APPEND",
            create_mode="CREATE_NEVER",
            ignore="ignored",
            format="CSV")

    def test_init_with_project_id(self):
        project = "here-is-project-id"
        bq = BQ(project)

        assert project == bq._project
        assert bq._mode == "WRITE_APPEND"
        assert bq._create_mode == "CREATE_IF_NEEDED"
        assert bq._format == "NEWLINE_DELIMITED_JSON"

    def test_init_with_kwargs(self):
        assert self.bq._table == "dataset.table"
        assert self.bq._gcs == "gs://abc"
        assert self.bq._sql == "select *"
        assert self.bq._schema== self.schema
        assert self.bq._mode == "WRITE_APPEND"
        assert self.bq._create_mode == "CREATE_NEVER"
        assert self.bq._format == "CSV"

        with pytest.raises(AttributeError):
            self.bq.ignore

    def test_change_attribute_after_init(self):
        self.bq.table("new.table")
        assert self.bq._table == "new.table"

        self.bq.gcs("gs://new")
        assert self.bq._gcs == "gs://new"

        self.bq.sql("with new")
        assert self.bq._sql == "with new"

        self.bq.mode("WRITE_TRUNCATE")
        assert self.bq._mode == "WRITE_TRUNCATE"

        self.bq.format("NEWLINE_DELIMITED_JSON")
        assert self.bq._format == "NEWLINE_DELIMITED_JSON"

    def test_invalid_gcs(self):
        with pytest.raises(ValueError):
            self.bq.gcs("not_a_gcs_prefix")

        with pytest.raises(ValueError):
            _ = BQ(project="proj_id", gcs="not_a_gcs_prefix")

    def test_invalid_sql(self):
        with pytest.raises(ValueError):
            self.bq.sql("insert into")

        with pytest.raises(ValueError):
            _ = BQ(project="proj_id", sql="delete from ...")

    def test_invalid_schema(self):
        with pytest.raises(TypeError):
            self.bq.schema("not a list")

        with pytest.raises(TypeError):
            _ = BQ(project="proj_id", schema="not a list")

    def test_invalid_mode(self):
        with pytest.raises(ValueError):
            self.bq.mode("wrong mode")

        with pytest.raises(ValueError):
            _ = BQ(project="proj_id", mode="wrong mode")

    def test_invalid_create_mode(self):
        with pytest.raises(ValueError):
            self.bq.create_mode("wrong mode")

        with pytest.raises(ValueError):
            _ = BQ(project="proj_id", create_mode="wrong mode")

    def test_invalid_format(self):
        with pytest.raises(ValueError):
            self.bq.format("wrong format")

        with pytest.raises(ValueError):
            _ = BQ(project="proj_id", format="wrong mode")