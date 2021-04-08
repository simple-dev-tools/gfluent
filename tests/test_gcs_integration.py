import os
from tempfile import NamedTemporaryFile, TemporaryDirectory
import unittest


from google.cloud import storage

from gfluent import GCS


class TestGCSIntegration(unittest.TestCase):
    def setUp(self):
        self.bucket = "johnny-trading-data"
        self.prefix = "sit-temp"
        self.project_id = os.environ.get("PROJECT_ID")

        self.tf = NamedTemporaryFile()

        with open(self.tf.name, 'w') as f:
            f.write("hello")

        os.makedirs("/tmp/sit-temp", exist_ok=True)

    def tearDown(self):
        GCS(self.project_id).bucket(self.bucket).prefix(self.prefix).delete()

    def test_upload_single(self):
        gcs = GCS(project=self.project_id)
        gcs.bucket(self.bucket).prefix(self.prefix).local(self.tf.name).upload()


    def test_upload_many(self):
        gcs = GCS(project=self.project_id)
        current_path = os.path.dirname(os.path.abspath(__file__))
        (
            gcs.bucket(self.bucket)
            .prefix("sit-temp")
            .local(os.path.join(current_path, "..", "docs"))
            .upload()
        )

    def test_download_many(self):
        current_path = os.path.dirname(os.path.abspath(__file__))
        # upload first
        (
            GCS(self.project_id).bucket(self.bucket)
            .prefix(self.prefix)
            .local(os.path.join(current_path, "..", "docs"))
            .upload()
        )

        # download
        (
            GCS(self.project_id).bucket(self.bucket)
            .prefix(self.prefix + '/')
            .local("/tmp")
            .download()
        )

    def test_download_single(self):
        # upload first
        with open(os.path.join('/', "tmp", "tempfile.txt"), 'w') as f:
            f.write("hello world")

        (
            GCS(self.project_id).bucket(self.bucket)
            .prefix(self.prefix)
            .local(os.path.join('/', "tmp", "tempfile.txt"))
            .upload()
        )

        # download
        (
            GCS(self.project_id).bucket(self.bucket)
            .prefix(self.prefix + '/' + "tempfile.txt")
            .local("/tmp")
            .download()
        )