"""A fluent style Storage client
"""
import logging
import os
from typing import List

from google.cloud.exceptions import NotFound
from google.cloud import storage

logger = logging.getLogger(__name__)

class GCS(object):
    __required_setting = {
        "local": "local full path",
        "bucket": "the bucket name without gs://",
        "prefix": "remote prefix"
    }
    def __init__(self, project: str, **kwargs):
        if not isinstance(project, str) or project is None:
            raise ValueError("project id must be provided to init the BQ")

        self._project = project
        self._client = storage.Client(project=project)

        for attr in kwargs:
            if attr in GCS.__required_setting.keys():
                getattr(self, attr)(kwargs[attr])
                # setattr(self, f"_{attr}", kwargs[attr])
            else:
                logger.warning(f"Ignored argument `{attr}`")

    def __repr__(self):
        return f"{GCS(project=self._project)}"

    def local(self, path: str, suffix: str = None):
        """Specify the local path, could be a directory or a file

        :param path: directory or file
        :type path: str

        :param path: the suffix of included files
        :type path: str, Optional

        :raises ValueError: if path not found as a file or directory 
        """
        self._local = path

        if os.path.isfile(path):
            self._local_files = [path]
        elif os.path.isdir(path):
            if suffix:
                self._local_files = [os.path.join(path, x) for x in os.listdir(path) 
                    if x.endswith(suffix) and os.path.isfile(os.path.join(path, x))]
            else:
                self._local_files = [os.path.join(path, x) for x in os.listdir(path) 
                    if os.path.isfile(os.path.join(path, x))]
        else:
            raise ValueError(f"{path} is not a dir nor a file")

        return self

    def bucket(self, bucket: str):
        """Specify the bucket name without gs://

        :param bucket: bucket name without gs://
        :type bucket: str
        """
        if bucket.startswith("gs://"):
            self._bucket = bucket[5:]
        else:
            self._bucket = bucket
        
        return self

    def prefix(self, prefix:str):
        """Specify the blob prefix

        :param prefix: without the ending /
        :type prefix: str
        """
        self._prefix = prefix

        return self
    
    def upload(self):
        """Upload file(s) to GCS with given prefix
        """
        bucket = self._client.bucket(self._bucket)

        for f in self._local_files:
            basename = os.path.basename(f)
            blob = bucket.blob(f"{self._prefix}/{basename}")
            logger.info(f"uploading {f} to gs://{self._bucket}/{blob.name}")
            blob.upload_from_filename(f)

    def download(self):
        """Download file from the given prefix to local folder

        The prefix of the blob object will be ignored,

        ``gs://bucket/folder1/abc.txt`` will be downloaded to ``/var/temp/abc.txt``
        if the ``.local('var/temp')`` is set.
        """
        if not os.path.isdir(self._local):
            raise ValueError(f"{self._local} must be a dir for download")

        bucket = self._client.bucket(self._bucket)
        blobs = bucket.list_blobs(prefix=self._prefix, delimiter='/')

        for blob in blobs:
            if not blob.name.endswith('/'):
                destination_uri = os.path.join(self._local, os.path.basename(blob.name))
                logger.info(f"downloading {blob.name} to {destination_uri}")
                blob.download_to_filename(destination_uri)
            else:
                logger.warning(f"Skipped the blob = {blob.name}, which is a directory")


    def delete(self):
        if "_prefix" not in self.__dict__:
            raise ValueError("_prefix must be specified for delete()")

        bucket = self._client.bucket(self._bucket)
        blobs_to_delete = [blob for blob in bucket.list_blobs(prefix=self._prefix)]

        with self._client.batch():
            for blob in blobs_to_delete:
                logger.warning(f"deleting gs://{self._bucket}/{blob.name}")
                blob.delete()