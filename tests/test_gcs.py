import os
from unittest.mock import ANY
from unittest.mock import call
from unittest.mock import patch

import pytest
from google.cloud import storage

from gfluent import GCS


@pytest.fixture()
def dummy_json_files(tmp_path):
    """Write 10 json files and 5 csv files to the temp dir"""
    for i in range(10):
        with open(f"{tmp_path}/{i}.json", "w") as f:
            f.write(f"dummy {i}\n")

    for i in range(5):
        with open(f"{tmp_path}/{i}.csv", "w") as f:
            f.write(f"dummy {i}\n")

    return tmp_path


def test_init_with_project_id():
    project_id = "here-is-the-project_id"
    with patch("gfluent.gcs.storage.Client", autospec=True):
        gcs = GCS(project_id)

    assert gcs._project == project_id


def test_init_with_kwargs_local_dir(tmp_path):
    project_id = "here-is-the-project_id"
    with patch("gfluent.gcs.storage.Client", autospec=True):
        gcs = GCS(
            project_id, local=tmp_path, bucket="gs://bucket", prefix="how/are/you"
        )

    assert gcs._project == project_id
    assert gcs._local == tmp_path
    assert gcs._bucket == "bucket"  # the prefix gs:// will be dropped
    assert gcs._prefix == "how/are/you"


def test_init_with_kwargs_local_files_and_suffix(dummy_json_files):
    """If the suffix is provided, all files with the suffix in the given directory
    will be saved to the _local_files as full path
    """
    project_id = "here-is-the-project_id"
    with patch("gfluent.gcs.storage.Client", autospec=True):
        gcs = GCS(project_id, bucket="gs://bucket", prefix="how/are/you").local(
            dummy_json_files, "json"
        )

    assert gcs._project == project_id
    assert gcs._bucket == "bucket"  # the prefix gs:// will be dropped
    assert gcs._prefix == "how/are/you"
    assert len(gcs._local_files) == 10
    assert f"{dummy_json_files}/1.json" in gcs._local_files


def test_init_with_kwargs_local_files_no_suffix(dummy_json_files):
    """If the suffix is None, all files in the given directory will be saved
    to the _local_files as full path
    """
    project_id = "here-is-the-project_id"
    with patch("gfluent.gcs.storage.Client", autospec=True):
        gcs = GCS(
            project_id,
            local=dummy_json_files,
            bucket="gs://bucket",
            prefix="how/are/you",
        )

    assert gcs._project == project_id
    assert gcs._bucket == "bucket"  # the prefix gs:// will be dropped
    assert gcs._prefix == "how/are/you"
    assert len(gcs._local_files) == 15
    assert f"{dummy_json_files}/1.json" in gcs._local_files
    assert f"{dummy_json_files}/1.csv" in gcs._local_files


@patch("gfluent.gcs.storage.Client", autospec=True)
def test_upload(obj, dummy_json_files):
    project_id = "here-is-the-project_id"
    gcs = GCS(
        project_id, local=dummy_json_files, bucket="gs://bucket", prefix="how/are/you"
    )
    gcs.upload()

    blob_calls = []
    for i in range(10):
        blob_calls.append(call(os.path.join("how/are/you", f"{i}.json")))
    for i in range(5):
        blob_calls.append(call(os.path.join("how/are/you", f"{i}.csv")))

    upload_calls = []
    for i in range(10):
        upload_calls.append(call(os.path.join(dummy_json_files, f"{i}.json")))
    for i in range(5):
        upload_calls.append(call(os.path.join(dummy_json_files, f"{i}.csv")))

    assert len(gcs._local_files) == 15
    # The Client is called
    obj.assert_called_once_with(project=project_id)
    # The .bucket() called once
    obj.return_value.bucket.assert_called_once_with("bucket")

    # The .blob() called 15 times
    (
        obj.return_value.bucket.return_value.blob.assert_has_calls(
            blob_calls, any_order=True
        )
    )

    # The .upload_from_filename() called 15 times
    (
        obj.return_value.bucket.return_value.blob.return_value.upload_from_filename.assert_has_calls(
            upload_calls, any_order=True
        )
    )


@patch("gfluent.gcs.storage.Client", autospec=True)
def test_download_exception(obj):
    project_id = "here-is-the-project_id"
    with pytest.raises(ValueError):
        _ = GCS(
            project_id,
            local="non_valid_path/path",
            bucket="gs://bucket",
            prefix="how/are/you",
        )


@patch("gfluent.gcs.storage.Client", autospec=True)
@patch("gfluent.gcs.storage.blob.Blob", autospec=True)
def test_download_with_actual_files(blob_mock, client_mock, tmp_path):
    project_id = "here-is-the-project_id"
    gcs = GCS(project_id, local=tmp_path, bucket="gs://bucket", prefix="how/are/you")

    client_mock.return_value.list_blobs.return_value = [
        storage.blob.Blob(name="xyz", bucket="bucket"),
        storage.blob.Blob(name="123", bucket="bucket"),
    ]

    # This is because the logger will use {blob.name} value
    blob_mock.return_value.name = "abc.json"

    gcs.download()

    download_calls = [
        call(os.path.join(tmp_path, "abc.json")),
        call(os.path.join(tmp_path, "abc.json")),
    ]

    client_mock.assert_called_once_with(project=project_id)
    client_mock.return_value.bucket.assert_called_once_with("bucket")
    (
        client_mock.return_value.list_blobs.assert_called_once_with(
            ANY, prefix="how/are/you/", delimiter="/"
        )
    )

    blob_mock.return_value.download_to_filename.assert_has_calls(
        download_calls, any_order=True
    )


@patch("gfluent.gcs.storage.Client", autospec=True)
@patch("gfluent.gcs.storage.blob.Blob", autospec=True)
def test_download_skip_directory(blob_mock, client_mock, tmp_path):
    project_id = "here-is-the-project_id"
    gcs = GCS(project_id, local=tmp_path, bucket="gs://bucket", prefix="how/are/you")

    client_mock.return_value.list_blobs.return_value = [
        storage.blob.Blob(name="xyz", bucket="bucket"),
        storage.blob.Blob(name="123", bucket="bucket"),
    ]

    # This is because the logger will use {blob.name} value
    blob_mock.return_value.name = "fake/directory/"

    gcs.download()

    client_mock.assert_called_once_with(project=project_id)
    client_mock.return_value.bucket.assert_called_once_with("bucket")
    (
        client_mock.return_value.list_blobs.assert_called_once_with(
            ANY, prefix="how/are/you/", delimiter="/"
        )
    )

    blob_mock.return_value.download_to_filename.assert_not_called()


@patch("gfluent.gcs.storage.Client", autospec=True)
def test_delete_exception(obj, tmp_path):
    project_id = "here-is-the-project_id"
    with pytest.raises(ValueError):
        gcs = GCS(project_id, bucket="gs://bucket")
        gcs.delete()


@patch("gfluent.gcs.storage.Client", autospec=True)
@patch("gfluent.gcs.storage.blob.Blob", autospec=True)
def test_delete(blob_mock, client_mock):
    project_id = "here-is-the-project_id"
    gcs = GCS(project_id, bucket="gs://bucket", prefix="how/are/you")

    client_mock.return_value.list_blobs.return_value = [
        storage.blob.Blob(name="xyz", bucket="bucket"),
        storage.blob.Blob(name="123", bucket="bucket"),
    ]

    # This is because the logger will use {blob.name} value
    blob_mock.return_value.name = "abc"

    gcs.delete()

    client_mock.assert_called_once_with(project=project_id)
    client_mock.return_value.batch.assert_called_once()

    client_mock.return_value.bucket.assert_called_once_with("bucket")
    (
        client_mock.return_value.list_blobs.assert_called_once_with(
            ANY, prefix="how/are/you/", delimiter="/"
        )
    )

    blob_mock.return_value.delete.assert_called()
