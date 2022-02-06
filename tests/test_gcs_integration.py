import os

import pytest

from gfluent import GCS


PROJECT_ID = "TBD"
BUCKET = "TBD"
PREFIX = "TBD"


@pytest.fixture
def make_up_file(tmp_path):
    filename = "data.txt"
    full_path = os.path.join(tmp_path, filename)
    with open(full_path, "w") as f:
        f.write("hello world\n")

    return full_path


@pytest.fixture
def make_up_many_files(tmp_path):
    for i in range(10):
        with open(os.path.join(tmp_path, f"file_{i}.txt"), "w") as f:
            f.write(f"The file {i}\n")

    return tmp_path


@pytest.mark.integtest
def test_upload_single_file(make_up_file):
    gcs = GCS(PROJECT_ID).local(make_up_file).bucket(BUCKET).prefix(PREFIX)
    gcs.upload()

    gcs.delete()


@pytest.mark.integtest
def test_upload_many_files_in_directory(make_up_many_files):
    gcs = GCS(PROJECT_ID).local(make_up_many_files).bucket(BUCKET).prefix(PREFIX)
    gcs.upload()

    gcs.delete()


@pytest.mark.integtest
def test_download_files(tmp_path, make_up_many_files):
    local_new_path = os.path.join(tmp_path, "saved")
    os.makedirs(local_new_path, exist_ok=True)

    # upload first
    gcs = GCS(PROJECT_ID).local(make_up_many_files).bucket(BUCKET).prefix(PREFIX)
    gcs.upload()

    # download them
    new_gcs = GCS(PROJECT_ID).local(local_new_path).bucket(BUCKET).prefix(PREFIX)
    new_gcs.download()

    for i in range(10):
        with open(os.path.join(local_new_path, f"file_{i}.txt"), "r") as f:
            content = f.read()
            assert content == f"The file {i}\n"

    # delete everything
    gcs.delete()
