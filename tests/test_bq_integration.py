import os

import pytest
from google.cloud import bigquery

from gfluent import BQ


class TestBQIntegration():
    def setup_class(self):
        self.dataset_name = "testing_bq"
        self.project_id = os.environ.get("PROJECT_ID")

        self.sql = """
            SELECT
                exchange,
                symbol,
                enabled
            FROM
                market_data.required_products
            LIMIT 5
        """

        self.table_name = f"{self.dataset_name}.testing_bq_table"

        self.bq = BQ(project=self.project_id)

        self.bq.delete_dataset(self.dataset_name)
        self.bq.create_dataset(self.dataset_name, location="EU")

    def teardown_class(self):
        self.bq.delete_dataset(self.dataset_name)

    def test_query(self):
        rows = self.bq.sql(self.sql).query()
        assert rows.total_rows == 5

    def test_query_load(self):

        # load the query result to table
        row_count = (self.bq
                    .table(self.table_name)
                    .sql(self.sql)
                    .query()
                    )

        # should only 5 rows
        assert row_count == 5

        # load again with append
        row_count = (
            self.bq
                .table(self.table_name)
                .sql(self.sql)
                .mode("WRITE_APPEND")
                .query()
        )
        
        rows = BQ(self.project_id).sql(f"select * from {self.table_name}").query()

        assert rows.total_rows == 10


    def test_truncate(self):
        self.bq.table(self.table_name).truncate()

        rows = BQ(self.project_id).sql(f"select * from {self.table_name}").query()

        assert rows.total_rows == 0

    def test_delete(self):
        self.bq.delete()
        assert self.bq.is_exist() is False

    def test_drop(self):
        row_count = (self.bq
                    .table(self.table_name)
                    .sql(self.sql)
                    .query()
                    )
        assert self.bq.is_exist() is True

        self.bq.drop()
        assert self.bq.is_exist() is False

    def test_is_exists(self):
        assert self.bq.is_exist() is False
        row_count = (self.bq
                    .table(self.table_name)
                    .sql(self.sql)
                    .query()
                    )
        assert self.bq.is_exist() is True
        assert row_count == 5

    def test_create_table(self):

        table = f"{self.dataset_name}.students"
        schema = [
            bigquery.SchemaField(
                name="name",
                field_type="STRING",
                mode="REQUIRED",
                description="student name"
            ),
            bigquery.SchemaField(
                name="age",
                field_type="INTEGER",
                mode="REQUIRED",
                description="student age"
            ),
        ]
        bq = BQ(project=self.project_id).table(table).schema(schema)

        assert bq.is_exist() is False

        bq.create()

        assert bq.is_exist() is True

