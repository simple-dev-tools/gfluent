import unittest
from os import path, getenv
from google.cloud import bigquery
import googleapiclient.discovery
from google.oauth2 import service_account
from gfluent import Sheet
from gfluent import BQ

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive'
]

SA_PATH = getenv("stocksa")


class TestSheetIntegration(unittest.TestCase):
    """please makesure stocksa, PROJECT_ID and SHEET_ID available in your local environments
    """

    def setUp(self):
        self.project_id = getenv("PROJECT_ID")
        self.sheet_id = getenv("SHEET_ID")
        self.table_upload_bq = "test123.testtable"
        self.table_upload_bq_no_range = "test123.testable_no_range"
        self.table_upload_bq_kwargs = "test123.testable_kwargs"
        self.worksheet = "Sheet1"
        self.range = "A:C"

    def test_upload_to_bq(self):

        sheet = Sheet(SA_PATH)
        bq = BQ(self.project_id, table=self.table_upload_bq)

        sheet.sheet_id(self.sheet_id) \
             .worksheet(self.worksheet) \
             .range(self.range) \
             .bq(bq) \
             .load()

    def test_upload_to_bq_no_range(self):

        sheet = Sheet(SA_PATH)
        bq = BQ(self.project_id, table=self.table_upload_bq_no_range)

        sheet.sheet_id(self.sheet_id) \
             .worksheet(self.worksheet) \
             .bq(bq) \
             .load()

    def test_upload_bq_kwargs(self):

        bq = BQ(self.project_id, table=self.table_upload_bq_kwargs)

        sheet = Sheet(SA_PATH,
                      sheet_id=self.sheet_id,
                      worksheet=self.worksheet,
                      range=self.range,
                      bq=bq)\
            .load()
