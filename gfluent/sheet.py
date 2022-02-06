"""A fluent style Google Sheet client
"""
import logging
import os
import re
from typing import List
from typing import Union

import googleapiclient.discovery
from google.api_core.exceptions import Conflict
from google.cloud import bigquery
from google.oauth2 import service_account

from gfluent import BQ

logger = logging.getLogger(__name__)

_GOOGLECREDENTIAL = service_account.Credentials


class Sheet(object):
    """The fluent-style Google Sheet for chaining class

    This ``Sheet`` class provides the interface to load Spreadsheet data to
    Bigquery table even in one line. The destiniation table must be a new table,
    and not exist in the same dataset.

    Examples:

    .. code-block:: python

        # use the headers from spread sheet and auto detect the type
        (
            Sheet('google-sa-credential-or-path')
            .sheet_id('your-sheet-id')
            .worksheet('sheet_name!A1:B100')    # provide the range in one-go
            .bq(BQ(projec_id='project-id', table='dataset.table')
        ).load()

        # use given schema definition
        schemas = [
            bigquery.SchemeField(...),
            bigquery.SchemeField(...),
            bigquery.SchemeField(...),
        ]
        (
            Sheet('google-sa-credential-or-path')
            # the sheet id will be extracted automatically
            .url('google-sheet-url')
            .worksheet('sheet_name')
            .range('A1:B100') # provide the range in separate call
            .bq(BQ(projec_id='project-id', table='dataset.table')
            .schema(schemas)
        ).load()

    :param credential_or_path: the ``service_account.Credentials`` object or file path
    :type credential_or_path: Union[Credentials, str]

    """

    __required_setting = {
        "sheet_id": "The Google sheet id",
        "worksheet": "The name of the google worksheet",
        "range": "the worksheet range",
        "bq": "the Bigquery connector",
        "schema": "The Bigquery Schema for the destination table",
    }

    def __init__(self, credential_or_path: Union[_GOOGLECREDENTIAL, str], **kwargs):
        SCOPES = [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive",
        ]

        if isinstance(credential_or_path, str) and os.path.isfile(credential_or_path):
            credentials = service_account.Credentials.from_service_account_file(
                credential_or_path, scopes=SCOPES
            )
        elif isinstance(credential_or_path, _GOOGLECREDENTIAL):
            credentials = credential_or_path
        else:
            raise ValueError(
                "A full path of gcs service account json file or google credential object"
            )
        self._service = googleapiclient.discovery.build(
            "sheets", "v4", credentials=credentials
        )

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
        if not isinstance(sheet_id, str) or len(sheet_id) < 40:
            raise TypeError(f"{sheet_id} is not a valid Google sheet id")

        self._sheet_id = sheet_id

        return self

    def url(self, url: str):
        """Pass the Google sheet URL

        :param url: The full URL of Google Sheet
        :type url: str
        """
        RE_URL = r"/spreadsheets/d/([a-zA-Z0-9-_]+)"
        if not isinstance(url, str) or not re.findall(RE_URL, url):
            raise ValueError("Please input valid url")

        self._sheet_id = re.findall(RE_URL, url)[0]

        return self

    def schema(self, schema: List[bigquery.SchemaField]):
        """Set the schema for desitnation table

        :param schema: The list of fields
        :type schema: List[bigquery.SchemaField]
        """

        self._schema = schema

        return self

    def worksheet(self, worksheet: str):
        """Specify the worksheet name with or without range

        The first row in the range is considered as ``header row``, and it is not
        able to be skipped.

        Valid values: ``sheet_name!A1:B3`` or just ``sheet_name``

        :param worksheet: The worksheet with range or only worksheet name
        :type worksheet: str
        """
        if "_sheet_id" not in self.__dict__:
            raise ValueError(".sheet_id() must be called before call .worksheet()")

        self._worksheet = worksheet

        return self

    def range(self, range: str):
        """Specify the worksheet data range in A1:B4 form

        The client library doesn't check if the range is valid, if any syntax
        error with the range, Google Sheet will raise the exception

        The first row in the range is considered as ``header row``, and it is not
        able to be skipped.

        :type range: str
        """

        if "_worksheet" not in self.__dict__:
            raise ValueError(
                ".range() should be called only after .worksheet() has been called"
            )

        if "!" in self._worksheet:
            raise ValueError(
                f"{self._worksheet} - range already included in the worksheet"
            )

        self._range = range

        return self

    def bq(self, bq: BQ):
        """use project id and other params to initial bq object

        :param bq: The ``BQ`` instance
        :type: :class:`gfluent.BQ`

        """
        if not isinstance(bq, BQ) or not bq:
            raise TypeError("bq must be an instance of BQ")

        self._bq = bq

        # pass the schema to BQ
        if "_schema" in self.__dict__:
            self._bq.schema(self._schema)

        return self

    def _worksheet_request(self):
        """To create the google sheet HttpReqeust"""
        if "_range" in self.__dict__:
            _worksheet_and_range = self._worksheet + "!" + self._range
        else:
            _worksheet_and_range = self._worksheet

        return (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=self._sheet_id, range=_worksheet_and_range)
        )

    def _load(self):
        """load Google Sheet Data to json object"""

        regexp = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

        sheet_result = self._worksheet_request().execute()
        if "values" not in sheet_result:
            raise ValueError("Empty Google Sheet, aborted")

        data = self._worksheet_request().execute()["values"]
        if not data[0]:
            raise ValueError("Empty Google Sheet column name, aborted")

        illegal_word = [word for word in data[0] if not regexp.search(word)]

        self._json_to_be_load = []

        if "_schema" not in self.__dict__:
            if illegal_word:
                raise ValueError(f"Field Name`{illegal_word[0]}` is illegal header")

            for d in data[1:]:
                self._json_to_be_load.append(dict(zip(data[0], d)))
        else:
            if len(data[0]) != len(self._schema):
                raise ValueError(
                    f"Schema defines {len(self._schema)} columns, header has {len(data[0])} columns"
                )
            headers = [x.name for x in self._schema]
            for d in data[1:]:
                for ind, value in enumerate(d):
                    if isinstance(value, str):
                        d[ind] = value.strip()
                self._json_to_be_load.append(dict(zip(headers, d)))

    def load(self, location: str = "US"):
        """Load the Data to BigQuery table

        Please use ``.bq.table()`` to set the destination table name, and the table
        must not exists, otherwise the ``Conflict`` exception will be raised.

        :param location: The BigQuery location, default is ``US``
        :type: str

        :raises google.api_core.exceptions.Conflict: table already
            exists exception.
        """

        if "_bq" not in self.__dict__ or "_worksheet" not in self.__dict__:
            raise ValueError(".worksheet() and .bq() must be called before run")
        if "_table" not in self._bq.__dict__:
            raise ValueError("bigquery table must be specify for the load")

        self._load()

        if self._bq.is_exist():
            raise Conflict(f"{self._bq._table} exists")

        if "_schema" not in self._bq.__dict__:

            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                source_format=self._bq._format,
            )

        else:
            job_config = bigquery.LoadJobConfig(
                schema=self._bq._schema, source_format=self._bq._format
            )
        load_job = self._bq._client.load_table_from_json(
            self._json_to_be_load,
            self._bq._table,
            location=location,
            job_config=job_config,
        )

        load_job.result()

        logger.info(
            f"{self._sheet_id} is loaded into {self._bq._project}.{self._bq._table}"
        )
