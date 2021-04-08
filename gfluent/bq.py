"""A fluent style BigQuery client
"""
import logging
from typing import List

from google.cloud.exceptions import NotFound
from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BQ(object):
    __required_setting = {
        "table": "The BigQuery full tablename with dataset",
        "gcs": "The GCS location with gs:// prefix",
        "sql": "SQL Statement should start with SELECT",
        "schema": "The BigQuery standard Schema structure",
        "mode": "override or append mode",
        "create_mode": "create or never create"
    }
    def __init__(self, project: str, **kwargs):
        """The fluent-style BigQuery client for chaining calls


        :param project_id: The GCP Project id
        :type project_id: str

        Example:
        
        .. code-block:: python

            # run the query and save to the table dataset.name
            bq = BQ(project='you-project-id', table='dataset.name')
            num_rows = bq.mode('CREATE_TRUNCATE').sql('select * from table').query()

            bq = BQ(project='you-project-id')

            rows = bq.sql('select id, name from abc.tab').query()
            for row in rows:
                print(row.id, row.name)

        """
        if not isinstance(project, str) or project is None:
            raise ValueError("project id must be provided to init the BQ")

        self._project = project
        self._client = bigquery.Client(project=project)

        self._mode = "WRITE_APPEND"
        self._create_mode = "CREATE_IF_NEEDED"

        for attr in kwargs:
            if attr in BQ.__required_setting.keys():
                getattr(self, attr)(kwargs[attr])
                # setattr(self, f"_{attr}", kwargs[attr])
            else:
                logger.warning(f"Ignored argument `{attr}`")
    
    def __repr__(self):
        return f"BQ(project={self._project})"
    
    def table(self, table: str):
        if not isinstance(table, str) or '.' not in table:
            raise ValueError(f"{table} is not look like dataset.table")

        self._table = table
        self.__table_id = f"{self._project}.{table}"

        return self

    
    def gcs(self, gcs: str):
        if not isinstance(gcs, str) or not gcs.startswith("gs://"):
            raise ValueError(f"{gcs} is not look like gs://bucket/path/")

        self._gcs = gcs

        return self

    def sql(self, sql: str):
        if not isinstance(sql, str) or not sql.strip().upper().startswith("SELECT"):
            raise ValueError(f"{sql} is not look like SELECT ...")

        self._sql = sql

        return self

    def schema(self, schema: List[bigquery.SchemaField]):
        if not isinstance(schema, list):
            raise TypeError("The `schema` should be List[bigquery.SchemaField]")

        self._schema = schema

        return self

        
    def mode(self, mode: str):
        """Set the bigquery `write_disposition` parameter

        WRITE_EMPTY	    This job should only be writing to empty tables.
        WRITE_TRUNCATE	This job will truncate table data and write from the beginning.
        WRITE_APPEND	This job will append to a table.

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
        """Set the bigquery `create_disposition` parameter

        CREATE_NEVER	This job should never create tables.
        CREATE_IF_NEEDED	This job should create a table if it doesn't already exist.

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
        """
        if "_table" in self.__dict__:
            return self._query_load()
        else:
            return self._query()

    def load(self):
        pass

    def export(self):
        pass

    def truncate(self):
        """Delete all rows in a table
        """
        if "_table" not in self.__dict__:
            raise ValueError("You must specify the table")

        sql_command = f"TRUNCATE TABLE {self.__table_id}"
        logger.info(f"truncate is called, sql = {sql_command}")
        self._client.query(sql_command).result()

    def create(self):
        pass

    def delete(self):
        """Delete a given table
        """
        if "_table" not in self.__dict__:
            raise ValueError("You must specify the table")

        self._client.delete_table(self.__table_id, not_found_ok=True)

    def create_dataset(self, dataset: str, location="US", timeout=30):
        """Create the dataset
        """
        dataset_id = f"{self._project}.{dataset}"

        ds_ref = bigquery.Dataset(dataset_id)
        ds_ref.location = location

        ds = self._client.create_dataset(ds_ref, timeout=30)
        logger.info(f"Successful created dataset {ds.dataset_id}")


    def delete_dataset(self, dataset: str):
        """Create the dataset
        """
        dataset_id = f"{self._project}.{dataset}"

        self._client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
        logger.info(f"Successful deleted dataset {dataset_id}")


    def is_exist(self) -> bool:
        """Check if a given table exists
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