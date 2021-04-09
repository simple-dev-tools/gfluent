import unittest
from os import path

from google.cloud import bigquery

from gfluent import BQ


class TestBQ(unittest.TestCase):
    def test_init_with_project_id(self):
        project = "here-is-project-id"
        bq = BQ(project)

        self.assertEqual(project, bq._project)

    def test_default_modes(self):
        project = "here-is-project-id"
        bq = BQ(project)

        self.assertEqual(project, bq._project)
        self.assertEqual(bq._mode, "WRITE_APPEND")
        self.assertEqual(bq._create_mode, "CREATE_IF_NEEDED")

    def test_format_keyword(self):
        project = "here-is-project-id"
        bq = BQ(project, format="format")
        self.assertEqual(bq._format, "format")

        bq1 = BQ(project)
        bq1.format("abc")
        self.assertEqual(bq1._format, "abc")

    def test_init_with_kwargs(self):
        project = "here-is-project-id"
        schema = [
            bigquery.SchemaField(
                "exec_id", "INTEGER", "desc"
            ),
            bigquery.SchemaField(
                "name", "STRING", "desc"
            ),
        ]
        bq = BQ(project, 
            table="dataset.table", 
            gcs="gs://abc",
            sql="select *", 
            schema=schema,
            mode="WRITE_APPEND",
            create_mode="CREATE_NEVER",
            ignore="ignored")

        bq2 = BQ(project)
        holder = bq2.table("dataset.table").gcs("gs://abc")

        bq1 = BQ(project)
        bq1.table("here-is.table")

        self.assertEqual(project, bq._project)
        self.assertEqual(bq._sql, "select *")
        self.assertEqual(bq._table, "dataset.table")
        self.assertEqual(holder._table, "dataset.table")
        self.assertEqual(bq._gcs, "gs://abc")
        self.assertEqual(holder._gcs, "gs://abc")
        self.assertEqual(bq._mode, "WRITE_APPEND")
        self.assertEqual(bq._create_mode, "CREATE_NEVER")

        self.assertEqual(bq1._table, "here-is.table")

    def test_exception(self):
        with self.assertRaises(ValueError):
            _ = BQ("abc", gcs="not")

        with self.assertRaises(ValueError):
            _ = BQ("abc").mode("ABC")