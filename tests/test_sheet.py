import unittest
from os import path, getenv
from google.cloud import bigquery
import googleapiclient.discovery
from google.oauth2 import service_account
from gfluent import Sheet
from gfluent import BQ

_GOOGLESERVICE = googleapiclient.discovery.Resource

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
]

SA_PATH = getenv("stocksa")
googleSheetType = googleapiclient.http.HttpRequest


class TestSheet(unittest.TestCase):
    def test_init_with_SA_Path(self):
        sheet = Sheet(SA_PATH)

        self.assertEqual(_GOOGLESERVICE, sheet._service.__class__)

    def test_init_with_cred(self):

        cred = service_account.Credentials.from_service_account_file(
            SA_PATH, scopes=SCOPES)
        sheet = Sheet(cred)

        self.assertEqual(cred, sheet._sheet_cred)

    def test_sheet_id_keyword(self):

        sheet = Sheet(SA_PATH).sheet_id("abc")

        self.assertEqual(sheet._sheet_id, "abc")

    def test_worksheet_keyword(self):

        sheet = Sheet(SA_PATH).sheet_id("abc").worksheet("A:C")
        self.assertEqual(sheet._worksheet.__class__, googleSheetType)

    def test_with_kwargs(self):
        bq_project = 'here-is-project-id'
        table = "dataset.table"
        bq = BQ(bq_project, table=table)
        sheet = Sheet(SA_PATH,
                      sheet_id="abc",
                      worksheet="A:C",
                      bq=bq
                      )
        self.assertEqual(sheet._sheet_id, "abc")
        self.assertEqual(sheet._worksheet.__class__, googleSheetType)
        self.assertEqual(bq._project, bq_project)
        self.assertEqual(bq._table, table)

    def test_exception(self):
        with self.assertRaises(ValueError):
            _ = Sheet(SA_PATH).sheet_id(123)

        with self.assertRaises(ValueError):
            _ = Sheet(SA_PATH).worksheet("132")

        with self.assertRaises(ValueError):
            _ = Sheet(SA_PATH,
                      sheet_id="abc",
                      worksheet="A:C",
                      bq="bq"
                      )
