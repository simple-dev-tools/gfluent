import os
from unittest.mock import ANY
from unittest.mock import call
from unittest.mock import patch

import pytest

from gfluent import BQ
from gfluent import Sheet


@pytest.fixture()
def dummy_json_key(tmp_path):
    full_path = os.path.join(tmp_path, "dummy-key.json")
    with open(full_path, "w") as f:
        f.write("dummy\n")

    return full_path


@patch(
    "gfluent.sheet.googleapiclient.discovery.build",
    autospec=True,
    return_value="_service",
)
@patch("gfluent.sheet.service_account.Credentials", autospec=True)
def test_init_sheet_with_non_kwargs(cred_mock, build_mock, dummy_json_key):
    s = Sheet(dummy_json_key)
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]
    cred_mock.from_service_account_file.assert_called_once_with(
        dummy_json_key, scopes=SCOPES
    )
    build_mock.return_value = "_service"

    # normal sequence
    s.sheet_id("1TpviErgA8xiyCaY0iOIN1XfK0QIm4vcTulmtowaC7NM")
    assert s._sheet_id == "1TpviErgA8xiyCaY0iOIN1XfK0QIm4vcTulmtowaC7NM"

    s.worksheet("my_name")
    assert s._worksheet == "my_name"

    s.range("A1:B1")
    assert s._range == "A1:B1"


@patch(
    "gfluent.sheet.googleapiclient.discovery.build",
    autospec=True,
    return_value="_service",
)
@patch("gfluent.sheet.service_account.Credentials", autospec=True)
@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_init_sheet_with_kwargs(bq_mock, cred_mock, build_mock, dummy_json_key):
    bq = BQ("some-project-id")
    sheet_id = "1TpviErgA8xiyCaY0iOIN1XfK0QIm4vcTulmtowaC7NM"
    s = Sheet(dummy_json_key, sheet_id=sheet_id, worksheet="sheet_name!A1:B100", bq=bq)
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]
    cred_mock.from_service_account_file.assert_called_once_with(
        dummy_json_key, scopes=SCOPES
    )
    build_mock.return_value = "_service"

    assert s._sheet_id == "1TpviErgA8xiyCaY0iOIN1XfK0QIm4vcTulmtowaC7NM"
    assert s._worksheet == "sheet_name!A1:B100"


@patch(
    "gfluent.sheet.googleapiclient.discovery.build",
    autospec=True,
    return_value="_service",
)
@patch("gfluent.sheet.service_account.Credentials", autospec=True)
@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_init_sheet_id_exceptions(bq_mock, cred_mock, build_mock, dummy_json_key):
    bq = BQ("some-project-id")
    sheet_id = "not-a-valid-sheet"
    with pytest.raises(TypeError):
        _ = Sheet(
            dummy_json_key, sheet_id=sheet_id, worksheet="sheet_name!A1:B100", bq=bq
        )


@patch(
    "gfluent.sheet.googleapiclient.discovery.build",
    autospec=True,
    return_value="_service",
)
@patch("gfluent.sheet.service_account.Credentials", autospec=True)
@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_init_sheet_from_url(bq_mock, cred_mock, build_mock, dummy_json_key):
    bq = BQ("some-project-id")
    url = "https://docs.google.com/spreadsheets/d/1TpviErgA8xiyCaY0iOIN1XfK0QIm4vcTulmtowaC7NM/edit#gid=1970528798"
    s = Sheet(dummy_json_key, bq=bq)
    s.url(url)

    assert s._sheet_id == "1TpviErgA8xiyCaY0iOIN1XfK0QIm4vcTulmtowaC7NM"


@patch(
    "gfluent.sheet.googleapiclient.discovery.build",
    autospec=True,
    return_value="_service",
)
@patch("gfluent.sheet.service_account.Credentials", autospec=True)
@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_worksheet_called_before_sheet_id_exception(
    bq_mock, cred_mock, build_mock, dummy_json_key
):
    bq = BQ("some-project-id")
    with pytest.raises(ValueError):
        s = Sheet(dummy_json_key, bq=bq)
        s.worksheet("sheet_name!A1:B100")


@patch(
    "gfluent.sheet.googleapiclient.discovery.build",
    autospec=True,
    return_value="_service",
)
@patch("gfluent.sheet.service_account.Credentials", autospec=True)
@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_range_called_before_worksheet_exceptioin(
    bq_mock, cred_mock, build_mock, dummy_json_key
):
    bq = BQ("some-project-id")
    with pytest.raises(ValueError):
        s = Sheet(dummy_json_key, bq=bq)
        s.range("A1:B100")


@patch(
    "gfluent.sheet.googleapiclient.discovery.build",
    autospec=True,
    return_value="_service",
)
@patch("gfluent.sheet.service_account.Credentials", autospec=True)
@patch("gfluent.bq.bigquery.Client", autospec=True)
def test_range_called_when_worksheet_has_range_exceptioin(
    bq_mock, cred_mock, build_mock, dummy_json_key
):
    bq = BQ("some-project-id")
    with pytest.raises(ValueError):
        s = Sheet(dummy_json_key, bq=bq)
        s.worksheet("sheet_name!A1:B100")
        s.range("A1:B100")


@patch("gfluent.sheet.googleapiclient.discovery.build", autospec=True)
@patch("gfluent.sheet.service_account.Credentials", autospec=True)
def test_worksheet_request(cred_mock, build_mock, dummy_json_key):
    sheet_id = "1TpviErgA8xiyCaY0iOIN1XfK0QIm4vcTulmtowaC7NM"
    s = Sheet(dummy_json_key, sheet_id=sheet_id)
    s.worksheet("sheet_name")
    s.range("A1:B100")

    s._worksheet_request()

    build_mock.return_value.spreadsheets.assert_called_once()
    (
        build_mock.return_value.spreadsheets.return_value.values.return_value.get.assert_called_once_with(
            spreadsheetId=sheet_id, range="sheet_name!A1:B100"
        )
    )
