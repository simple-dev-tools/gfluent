import os
import unittest

from google.cloud import bigquery

from gfluent import BQ


class TestBQIntegration(unittest.TestCase):
    def setUp(self):
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

    def tearDown(self):
        self.bq.delete_dataset(self.dataset_name)

    def test_query(self):
        rows = self.bq.sql(self.sql).query()

        self.assertEqual(rows.total_rows, 5)

    def test_query_load(self):

        # load the query result to table
        row_count = (self.bq
                    .table(self.table_name)
                    .sql(self.sql)
                    .query()
                    )

        # should only 5 rows
        self.assertEqual(row_count, 5)

        # load again with append
        row_count = (
            self.bq
                .table(self.table_name)
                .sql(self.sql)
                .mode("WRITE_APPEND")
                .query()
        )
        
        rows = BQ(self.project_id).sql(f"select * from {self.table_name}").query()

        self.assertEqual(10, rows.total_rows)

    def test_truncate(self):

        # create the table first
        row_count = (self.bq
                    .table(self.table_name)
                    .sql(self.sql)
                    .query()
                    )

        # truncate it
        self.bq.table(self.table_name).truncate()

        rows = BQ(self.project_id).sql(f"select * from {self.table_name}").query()

        self.assertEqual(0, rows.total_rows)

    def test_delete(self):
        # create the table first
        row_count = (self.bq
                    .table(self.table_name)
                    .sql(self.sql)
                    .query()
                    )

        self.bq.delete()

        self.assertFalse(self.bq.is_exist())

    def test_is_exists(self):
        # create the table first
        row_count = (self.bq
                    .table(self.table_name)
                    .sql(self.sql)
                    .query()
                    )

        self.assertTrue(BQ(self.project_id).table(self.table_name).is_exist())