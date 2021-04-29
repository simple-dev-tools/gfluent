"""A fluent style Google Sheet client
"""
# from __future__ import annotations
import logging
from typing import List
from types import FunctionType
from gfluent import BQ
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from google.oauth2 import service_account
import googleapiclient.discovery
import re

logger = logging.getLogger(__name__)

_GOOGLECREDENTIAL = service_account.Credentials


def sheet_service(cls):
    """ Allow the the SHEET class to take service_account.Credentials object 
        and FULL path to the service account
    """
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive.readonly',
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive'
    ]

    def service_init(arg, **kwargs):
        if isinstance(arg, str):
            if ".json" in arg:
                credentials = service_account.Credentials.from_service_account_file(
                    arg, scopes=SCOPES)
                return cls(credentials, **kwargs)
            else:
                ValueError(
                    f"Please provided either FULL path of gcs service account json file or google credential object")
        elif isinstance(arg, _GOOGLECREDENTIAL):
            return cls(arg, **kwargs)
        else:
            raise ValueError(
                f"Please provided either FULL path of gcs service account json file or google credential object")
    return service_init


def typechecker(func):
    """
        Check function position params' type base on type hint
    """
    def wrapper(*args, **kwargs):

        func_anno = func.__annotations__

        if len(args) - 1 != len(func_anno):
            raise ValueError(
                f"passed positional parameter is {len(args)}, expected {len(func_anno)+1}")

        for arg in enumerate(args[1:]):
            if not isinstance(arg[1], list(func_anno.values())[arg[0]]):
                raise ValueError(
                    f"parameter `{list(func_anno.keys())[arg[0]]}`` type of `{list(func_anno.values())[arg[0]]}` required for `{func.__name__}`")

        return func(*args, **kwargs)
    return wrapper


@sheet_service
class Sheet(object):
    """The fluent-style Google Sheet for chaining class

    Example:

    .. code-block:: python

        #read the data from google sheet to tables 
        sheet = Sheet('google-sa-credential-or-path')
        sheet.sheet_id('your-sheet-id')
        .worksheet('workshet-tab-and-range')
        .bq('bq-projectid', table="your-dataset-table-name", schema="your-schema")
        .load()
    """

    __required_setting = {
        "sheet_id": "The Google sheet id",
        "worksheet": "The name of the google worksheet",
        "bq": "the Bigquery connector"
    }

    def __init__(self, sheet_cred: _GOOGLECREDENTIAL, **kwargs):
        """

        """
        self._sheet_cred = sheet_cred
        self._service = googleapiclient.discovery.build(
            'sheets', 'v4', credentials=self._sheet_cred)

        for attr in kwargs:
            if attr in self.__required_setting.keys():
                getattr(self, attr)(kwargs[attr])
            else:
                logger.warning(f"Ingored argument `{attr}`")

    @ typechecker
    def sheet_id(self, sheet_id: str):
        """Specify the UID of Google Sheet

        :type sheet_id: str
        """

        self._sheet_id = sheet_id

        return self

    @ typechecker
    def worksheet(self, worksheet: str):
        """Specify the either both sheet name and data range or either of them

        :type worksheet: str

        """
        if "_sheet_id" not in self.__dict__:
            raise ValueError(
                ".sheet_id() must be called before run")

        self._worksheet = self._service.spreadsheets().values().get(
            spreadsheetId=self._sheet_id, range=worksheet)

        return self

    @ typechecker
    def bq(self, bq: BQ):
        """use project id and other params to initial bq object

        :type: str

        """

        self._bq = bq

        return self

    def _load(self):
        """load Google Sheet Data to json object


        :raises Values error if
            - Empty Worksheet
            - No Worksheet Column names
            - wrong column name format, must start letters, numbers, and underscores, start with a letter or underscore

        """

        regexp = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

        sheet_result = self._worksheet.execute()
        if "values" not in sheet_result:
            raise ValueError("Empty Google Sheet, aborted")

        data = self._worksheet.execute()["values"]
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
        """ Load the Data to bigquery

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
