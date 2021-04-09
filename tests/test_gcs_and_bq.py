import os
from tempfile import NamedTemporaryFile, TemporaryDirectory
import unittest
import json


from google.cloud import storage

from gfluent import GCS, BQ


class TestGCSAndBQ(unittest.TestCase):
    def setUp(self):
        self.bucket = "johnny-trading-data"
        self.prefix = "sit-temp"
        self.project_id = os.environ.get("PROJECT_ID")

        self.tf = NamedTemporaryFile()

        self.td = TemporaryDirectory()

        data = [{"name": "Tom", "age": 19 }, {"name": "Sid", "age": 20}]

        with open(self.tf.name, 'w') as f:
            for item in data:
                f.write(json.dumps(item) + '\n')

        for name in range(10):
            with open(os.path.join(self.td.name, f"file_{name}.json"), 'w') as f:
                for item in data:
                    f.write(json.dumps(item) + '\n')

        gcs = GCS(project=self.project_id)

        # upload single file
        gcs.bucket(self.bucket).prefix(self.prefix).local(self.tf.name).upload()

        # upload more files
        gcs.bucket(self.bucket).prefix("batch-json").local(self.td.name).upload()

        self.uri = f"gs://{self.bucket}/{self.prefix}/{os.path.basename(self.tf.name)}"

        self.uris = f"gs://{self.bucket}/batch-json/*.json"


    def tearDown(self):

        self.td.cleanup()

        # clean all uploaded files
        GCS(self.project_id).bucket(self.bucket).prefix(self.prefix).delete()

        # clean all uploaded files
        GCS(self.project_id).bucket(self.bucket).prefix("batch-json").delete()

        # drop the created table
        BQ(self.project_id).table("temp.temp_table").delete()
        BQ(self.project_id).table("temp.batch-json").delete()


    def test_bq_load(self):
        bq = BQ(self.project_id)
        bq.table("temp.temp_table").gcs(self.uri)

        bq.load(location="EU")

    def test_bq_load_multiple(self):
        bq = BQ(self.project_id)
        bq.table("temp.batch-json").gcs(self.uris)

        bq.load(location="EU")