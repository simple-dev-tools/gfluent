"""A fluent style Google Sheet client
"""
import logging
import os
import re
from typing import Union

from google.cloud import bigquery
from google.oauth2 import service_account
import googleapiclient.discovery

from gfluent import BQ

logger = logging.getLogger(__name__)

_GOOGLECREDENTIAL = service_account.Credentials


class Sheet(object):
    """The fluent-style Google Sheet for chaining class

    Example:

    .. code-block:: python

        # read the data from google sheet to tables
        sheet = Sheet('google-sa-credential-or-path')
        sheet.sheet_id('your-sheet-id')
        .worksheet('workshet-tab-and-range')
        .bq('bq-projectid', table="your-dataset-table-name", schema="your-schema")
        .load()
    """

    __required_setting = {
        "sheet_id": "The Google sheet id",
        "worksheet": "The name of the google worksheet",
        "range": "the worksheet range",
        "bq": "the Bigquery connector",

    }

    def __init__(self, obj: Union[_GOOGLECREDENTIAL, str], **kwargs):
        SCOPES = [
            'https://www.googleapis.com/auth/spreadsheets.readonly',
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive.readonly',
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive'
        ]

        if isinstance(obj, str) and os.path.isfile(obj):
            credentials = service_account.Credentials.from_service_account_file(
                obj, scopes=SCOPES)
        elif isinstance(obj, _GOOGLECREDENTIAL):
            credentials = obj
        else:
            raise ValueError(
                f"Please provided either FULL path of gcs service account json file or google credential object")
        self._service = googleapiclient.discovery.build(
            'sheets', 'v4', credentials=credentials)

        for attr in kwargs:
            if attr in self.__required_setting.keys():
                getattr(self, attr)(kwargs[attr])
            else:
                logger.warning(f"Ingored argument `{attr}`")

    def sheet_id(self, sheet_id: str):
        """Specify the UID of Google Sheet

        :param sheet_id: The UID of Google Spreadsheet
        :type sheet_id: str
        """
        if not isinstance(sheet_id, str) or len(sheet_id) < 15:
            raise TypeError(f"{sheet_id} is not a valid Google sheet id")

        self._sheet_id = sheet_id

        return self

    def url(self, url: str):
        """Pass the Google sheet URL

        :param url: The full URL of Google Sheet
        :type url: str
        """
        pass

    def worksheet(self, worksheet: str):
        """Specify the worksheet name

        :param worksheet: the sheet name and data range
        :type worksheet: str
        """
        if "_sheet_id" not in self.__dict__:
            raise ValueError(
                ".sheet_id() must be called before run")

        self._worksheet = worksheet

        return self

    def range(self, range: str):
        """Specify the worksheet data range

        :type range: str
        """

        if "_worksheet" not in self.__dict__:
            raise ValueError(
                ".worksheet() must be called before run")
        self._range = range

        return self

    def bq(self, bq: BQ):
        """ use project id and other params to initial bq object

        :param bq: The ``BQ`` instance
        :type: :class:`gfluent.BQ`

        """
        if not isinstance(bq, BQ) or not bq:
            raise TypeError("bq must be an instance of BQ")

        self._bq = bq

        return self

    def _worksheet_request(self):
        """To create the google sheet HttpReqeust
        """
        if "_range" in self.__dict__:
            _worksheet_and_range = self._worksheet + "!" + self._range
        else:
            _worksheet_and_range = self._worksheet

        return self._service.spreadsheets().values().get(
            spreadsheetId=self._sheet_id, range=_worksheet_and_range)

    def _load(self):
        """load Google Sheet Data to json object


        :raises ValuesError: with following reasons
            - Empty Worksheet
            - No Worksheet Column names
            - wrong column name format, must start letters, numbers, and underscores, start with a letter or underscore

        """

        regexp = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

        sheet_result = self._worksheet_request().execute()
        if "values" not in sheet_result:
            raise ValueError("Empty Google Sheet, aborted")

        data = self._worksheet_request().execute()["values"]
        if not data[0]:
            raise ValueError("Empty Google Sheet column name, aborted")

        illegal_word = [word for word in data[0] if not regexp.search(word)]

        if illegal_word:
            raise ValueError(
                f"Field Name`{illegal_word[0]}` is illegal Fields must contain only letters, numbers, and underscores, start with a letter or underscore.")

        self._json_to_be_load = []
        for d in data[1:]:
            self._json_to_be_load.append(dict(zip(data[0], d)))

    # TODO: implement drop before load, always drop destination table before loading.
    def load(self, location: str = "US"):
        """Load the Data to BigQuery table
        """

        if "_bq" not in self.__dict__ or "_worksheet" not in self.__dict__:
            raise ValueError(
                ".worksheet() and .bq() must be called before run")
        if "_table" not in self._bq.__dict__:
            raise ValueError(
                "bigquery table must be specify for the load"
            )

        self._load()

        if "_schema" not in self._bq.__dict__:

            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                source_format=self._bq._format
            )

        else:
            job_config = bigquery.LoadJobConfig(
                schema=self._bq._schema,
                source_format=self._bq._format
            )
        load_job = self._bq._client.load_table_from_json(
            self._json_to_be_load, self._bq._table, location=location, job_config=job_config)

        load_job.result()

        logger.info(
            f"{self._sheet_id} is loaded into {self._bq._project}.{self._bq._table}")
