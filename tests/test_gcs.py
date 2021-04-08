import unittest
from os import path

from google.cloud import storage

from gfluent import GCS


class TestGCS(unittest.TestCase):
    def test_init_with_project_id(self):
        project = "here-is-project-id"
        gcs = GCS(project)

        self.assertEqual(project, gcs._project)
