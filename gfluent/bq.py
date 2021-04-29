"""A fluent style BigQuery client
"""
import logging
from typing import List

from google.cloud.exceptions import NotFound
from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BQ(object):
    """The fluent-style BigQuery client for chaining calls

    Example:
    
    .. code-block:: python

        # run the query and save to the table dataset.name
        bq = BQ(project='you-project-id', table='dataset.name')
        num_rows = bq.mode('CREATE_TRUNCATE').sql('select * from table').query()

        bq = BQ(project='you-project-id')

        rows = bq.sql('select id, name from abc.tab').query()
        for row in rows:
            print(row.id, row.name)

    Allowed additional arguments,

    .. code-block:: bash

        table: The BigQuery full tablename with dataset,
        gcs: The GCS location with gs:// prefix,
        sql: SQL Statement should start with SELECT,
        schema: The BigQuery standard Schema structure,
        mode: override or append mode,
        create_mode: create or never create

    :param project_id: The GCP Project id
    :type project_id: str

    :param kwargs: Additional arguments
    :type kwargs: dict

    """
    __required_setting = {
        "table": "The BigQuery full tablename with dataset",
        "gcs": "The GCS location with gs:// prefix",
        "sql": "SQL Statement should start with SELECT",
        "schema": "The BigQuery standard Schema structure",
        "mode": "override or append mode",
        "create_mode": "create or never create",
        "format": "the datafile format"
    }
    def __init__(self, project: str, **kwargs):
        if not isinstance(project, str) or project is None:
            raise ValueError("project id must be provided to init the BQ")

        self._project = project
        self._client = bigquery.Client(project=project)

        # default
        self._mode = "WRITE_APPEND"
        self._create_mode = "CREATE_IF_NEEDED"
        self._format = "NEWLINE_DELIMITED_JSON"

        for attr in kwargs:
            if attr in BQ.__required_setting.keys():
                getattr(self, attr)(kwargs[attr])
                # setattr(self, f"_{attr}", kwargs[attr])
            else:
                logger.warning(f"Ignored argument `{attr}`")
    
    def __repr__(self):
        return f"BQ(project={self._project})"
    
    def table(self, table: str):
        """Specify the table name with dataset.name format

        :param table: BigQuery full table name without project id
        :type table: str
        """
        if not isinstance(table, str) or '.' not in table:
            raise ValueError(f"{table} is not look like dataset.table")

        self._table = table
        self.__table_id = f"{self._project}.{table}"

        return self


    def format(self, format_: str):
        """Specify the format of import/export files, default NEWLINE_DELIMITED_JSON

        * ``AVRO`` Specifies Avro format.
        * ``CSV Specifies`` CSV format.
        * ``DATASTORE_BACKUP`` Specifies datastore backup format
        * ``NEWLINE_DELIMITED_JSON`` Specifies newline delimited JSON format.
        * ``ORC`` Specifies Orc format.
        * ``PARQUET`` Specifies Parquet format.

        :param format: [description]
        :type format: str
        """

        self._format = format_
        return self

    
    def gcs(self, gcs: str):
        """Specify the GCS location, single file or wildcard

        :param gcs: must start with ``gs://``
        :type gcs: str
        """
        if not isinstance(gcs, str) or not gcs.startswith("gs://"):
            raise ValueError(f"{gcs} is not look like gs://bucket/path/")

        self._gcs = gcs

        return self

    def sql(self, sql: str):
        """Specify the SQL statement

        Only one statement is allowed, and only support ``SELECT`` and ``WITH`` 
        as of now

        :param sql: must start with ``select``
        :type sql: str
        """
        if not isinstance(sql, str) \
            or (not sql.strip().upper().startswith("SELECT") \
            and not sql.strip().upper().startswith("WITH")):
            raise ValueError(f"{sql} is not look like SELECT or WITH ...")

        self._sql = sql

        return self

    def schema(self, schema: List[bigquery.SchemaField]):
        """Specify the table schema

        :param schema: A list of ``SchemaField`` definition
        :type schema: List[bigquery.SchemaField]
        """
        if not isinstance(schema, list):
            raise TypeError("The `schema` should be List[bigquery.SchemaField]")

        self._schema = schema

        return self

        
    def mode(self, mode: str):
        """Set the bigquery ``write_disposition`` parameter, default WRITE_APPEND

        * WRITE_EMPTY This job should only be writing to empty tables.
        * WRITE_TRUNCATE This job will truncate table data and write from the beginning.
        * WRITE_APPEND This job will append to a table.

        :param mode: must be one of above value
        :type mode: str
        :raises ValueError: when the value is not allowed
        """
        _allowed = ["WRITE_EMPTY", "WRITE_TRUNCATE", "WRITE_APPEND"]

        if not isinstance(mode, str) or mode not in _allowed:
            raise ValueError(f"{mode} is not one of {'|'.join(_allowed)}")

        self._mode = mode

        return self

    def create_mode(self, create_mode: str):
        """Set the bigquery ``create_disposition`` parameter, default CREATE_IF_NEEDED

        * CREATE_NEVER This job should never create tables.
        * CREATE_IF_NEEDED This job should create a table if it doesn't already exist.

        :param create_mode: must be one of above value
        :type create_mode: str
        """
        _allowed = ["CREATE_NEVER", "CREATE_IF_NEEDED"]

        if not isinstance(create_mode, str) or create_mode not in _allowed:
            raise ValueError(f"{create_mode} is not one of {'|'.join(_allowed)}")

        self._create_mode = create_mode

        return self

    def _query(self) -> bigquery.table.RowIterator:
        """Run the query and return rows
        """
        return self._client.query(self._sql).result()

    def _query_load(self) -> int:
        """Run the query and save result to table
        """
        job_config = bigquery.QueryJobConfig(
            destination=self.__table_id,
            write_disposition=self._mode,
            create_disposition=self._create_mode,
        )

        job = self._client.query(self._sql, job_config=job_config)

        r = job.result()

        return r.total_rows

    def query(self):
        """Run the given sql query, return rows or save to table

        If the ``table`` attribute is set, it will save the query result to that
        table,  otherwise it returns the BigQuery rows
        """
        if "_table" in self.__dict__:
            return self._query_load()
        else:
            return self._query()

    def load(self, location: str="US") -> int:
        """Run the ``LoadJob``, and return number of rows loaded

        ``.table()``, ``.gcs()`` must be called to run this method.
        ``.schema()`` is optional, if not specified, using ``autodetect`

        ``.mode()``, ``.create_mode()`` and ``.format()`` are optional, as they
        have default values.

        :param location: must be same as your dataset, default ``US``
        :type location: str
        """

        if "_table" not in self.__dict__ or "_gcs" not in self.__dict__:
            raise ValueError(".table() and .gcs() must be called before run")

        if "_schema" not in self.__dict__:
            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                source_format=self._format
            )
        else:
            job_config = bigquery.LoadJobConfig(
                schema=self._schema,
                source_format=self._format
            )

        load_job = self._client.load_table_from_uri(
            self._gcs,
            self.__table_id,
            location=location,
            job_config=job_config
        )

        load_job.result()

        return self._client.get_table(self.__table_id).num_rows


    def export(self):
        """Not implemented yet
        """
        pass

    def truncate(self):
        """Delete all rows in the given table

        ``.table()`` must be called before calling this method to speicfy which
        table to be truncated
        """
        if "_table" not in self.__dict__:
            raise ValueError("You must specify the table")

        sql_command = f"TRUNCATE TABLE {self.__table_id}"
        logger.info(f"truncate is called, sql = {sql_command}")
        self._client.query(sql_command).result()

    def create(self):
        """Not implemented yet
        """
        pass

    def drop(self):
        """Alias of ``.delete()``
        """
        self.delete()

    def delete(self):
        """Drop the given table

        ``.table()`` must be called before calling this method to speicfy which
        table to be dropped. No error will be raised if the table is not found.
        """
        if "_table" not in self.__dict__:
            raise ValueError("You must specify the table")

        logger.warning(f"deleting the table {self.__table_id}")
        self._client.delete_table(self.__table_id, not_found_ok=True)

    def create_dataset(self, dataset: str, location="US", timeout=30):
        """Create the given dataset

        :param dataset: The dataset id without project id
        :type dataset: str
        :param location: A BigQuery location, defaults to "US"
        :type location: str, optional
        :param timeout: The timeout in second, defaults to 30
        :type timeout: int, optional
        """
        dataset_id = f"{self._project}.{dataset}"

        ds_ref = bigquery.Dataset(dataset_id)
        ds_ref.location = location

        ds = self._client.create_dataset(ds_ref, timeout=30)
        logger.info(f"Successful created dataset {ds.dataset_id}")


    def delete_dataset(self, dataset: str):
        """Delete (or drop) the given dataset

        :param dataset: the dataset id, without project_id
        :type dataset: str
        """
        dataset_id = f"{self._project}.{dataset}"

        self._client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
        logger.info(f"Successful deleted dataset {dataset_id}")


    def is_exist(self) -> bool:
        """Check if a given table exists

        ``.table()`` must be called before calling this method to speicfy which
        table to be checked.
        """
        if "_table" not in self.__dict__:
            raise ValueError("You must specify the table")
        
        flag = True
        try:
            self._client.get_table(self.__table_id)
        except NotFound:
            logger.warning(f"Table {self.__table_id} not found")
            flag = False

        return flag