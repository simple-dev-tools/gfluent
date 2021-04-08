import os
import pytest

from google.cloud import bigquery
import googleapiclient.discovery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict

from gfluent import Sheet
from gfluent import BQ

TEST_UID = "1M917dp2-RhI5cC8BHAIp2yXDSuw4uaTzCBTOWmVXd5w"


class TestSheetIntegration():
    def setup_class(self):
        self.dataset_name = "gfluent_sheet_dataset"
        self.project_id = os.environ.get("PROJECT_ID")

        self.bq = BQ(project=self.project_id)

        self.bq.delete_dataset(self.dataset_name)
        self.bq.create_dataset(self.dataset_name, location="EU")

    def teardown_class(self):
        self.bq.delete_dataset(self.dataset_name)

    def test_sheet_load(self):
        sheet = Sheet(
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        ).sheet_id(TEST_UID).worksheet("data!A1:B4")

        self.bq.table(f"{self.dataset_name}.target_table")

        sheet.bq(self.bq).load(location="EU")

        rows = BQ(project=self.project_id).sql(
            f"select * from {self.dataset_name}.target_table"
        ).query()

        assert rows.total_rows == 3

    def test_sheet_load_with_schema(self):
        _sheet = "no-header"
        _range = "A:B"
        _tb = "no_header"

        sheet = Sheet(
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        ).sheet_id(TEST_UID).worksheet(_sheet).range(_range).schema(
            [
                bigquery.SchemaField(
                    name="name",
                    field_type="STRING",
                    mode="REQUIRED",
                    description="student name"
                ),
                bigquery.SchemaField(
                    name="age",
                    field_type="STRING",
                    mode="REQUIRED",
                    description="student age as string"
                ),
            ]
        )

        self.bq.table(f"{self.dataset_name}.{_tb}")

        sheet.bq(self.bq).load(location="EU")

        rows = BQ(project=self.project_id).sql(
            f"select * from {self.dataset_name}.{_tb}"
        ).query()

        assert rows.total_rows == 3

    def test_sheet_load_exist_exception(self):
        _tb = "duplicated_table_name"

        sheet = Sheet(
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        ).sheet_id(TEST_UID).worksheet("data!A1:B4")

        self.bq.table(f"{self.dataset_name}.{_tb}")

        # create the table
        table = f"{self.dataset_name}.{_tb}"
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
        bq = BQ(project=self.project_id).table(table).schema(schema).create()

        with pytest.raises(Conflict):
            sheet.bq(self.bq).load(location="EU")