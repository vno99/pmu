import json
from datetime import date, datetime
from unittest import mock

import pytest
import requests

from services.service_pmu import (
    _file_date,
    _get_data,
    _get_reunions_courses,
    fetch_course_pmu,
    fetch_participants_pmu,
    normalize_date,
    resolve_dag_date,
)

TODAY = datetime.now().date()


@pytest.mark.parametrize("une_date, expected", [
    (None, TODAY),
    ("", TODAY),
    ("None", TODAY),
    ("null", TODAY),
    ("2026-04-15", date(2026, 4, 15)),
    ("2026-12-31", date(2026, 12, 31)),
])
def test_normalize_date(une_date, expected):
    assert normalize_date(une_date) == expected


@pytest.mark.parametrize("une_date", [
    "15042026",    # format inversé (jour 15 / mois 20)
    "20260415",    # sans tirets
    "2026-4-5",    # strptime accepte sans zéro → round-trip le rejette
    "2026-13-01",  # 13e mois
    "2026-04-32",  # 32 avril
    "abc",
])
def test_normalize_date_invalid(une_date):
    with pytest.raises(ValueError):
        normalize_date(une_date)


@pytest.mark.parametrize("une_date, expected", [
    ("2026-04-15", "20260415"),
    (date(2026, 4, 15), "20260415"),
    (datetime(2026, 4, 15, 10, 30), "20260415"),
])
def test_file_date(une_date, expected):
    assert _file_date(une_date) == expected


@pytest.mark.parametrize("une_date, fallback, expected", [
    # Airflow rend None en chaîne "None" dans les templates / conf des
    # TriggerDagRunOperator : ces sentinelles doivent replier sur la date de secours
    # (le logical_date du run), jamais être transmises telles quelles à dbt.
    (None, "2026-08-31", "2026-08-31"),
    ("", "2026-08-31", "2026-08-31"),
    ("None", "2026-08-31", "2026-08-31"),
    ("null", "2026-08-31", "2026-08-31"),
    # Une date réellement fournie est conservée, quelle que soit la date de secours.
    ("2026-08-31", "2026-08-01", "2026-08-31"),
    (date(2026, 8, 31), "2026-08-01", date(2026, 8, 31)),
])
def test_resolve_dag_date(une_date, fallback, expected):
    assert resolve_dag_date(une_date, fallback) == expected


def _fake_response(payload, status_error=None):
    """Réponse requests factice : raise_for_status() no-op (ou lève) et json() contrôlé."""
    resp = mock.Mock()
    if status_error is not None:
        resp.raise_for_status.side_effect = status_error
    else:
        resp.raise_for_status.return_value = None
    resp.json.return_value = payload
    return resp


# --- fetch_course_pmu ---

def test_fetch_course_pmu_writes_json(tmp_path):
    payload = {"programme": {"reunions": []}}
    (tmp_path / "course").mkdir()

    with mock.patch("services.service_pmu.OUTPUT_DIR", str(tmp_path) + "/"), \
         mock.patch("services.service_pmu.requests.get", return_value=_fake_response(payload)) as mock_get:
        fetch_course_pmu("2026-04-15")

    output_file = tmp_path / "course" / "20260415_course.json"
    assert output_file.exists()
    assert json.loads(output_file.read_text(encoding="utf-8")) == payload

    args, kwargs = mock_get.call_args
    assert args[0] == "https://online.turfinfo.api.pmu.fr/rest/client/61/programme/15042026"
    assert kwargs["params"] == {"meteo": True, "specialisation": "INTERNET"}
    assert kwargs["timeout"] == 10


def test_fetch_course_pmu_http_error_no_file(tmp_path):
    (tmp_path / "course").mkdir()

    with mock.patch("services.service_pmu.OUTPUT_DIR", str(tmp_path) + "/"), \
         mock.patch(
             "services.service_pmu.requests.get",
             return_value=_fake_response(None, status_error=requests.exceptions.HTTPError("boom")),
         ):
        with pytest.raises(requests.exceptions.HTTPError):
            fetch_course_pmu("2026-04-15")

    assert not list((tmp_path / "course").iterdir())


# --- fetch_participants_pmu ---

def test_fetch_participants_pmu_writes_json(tmp_path):
    payload = {"participants": []}
    (tmp_path / "participant").mkdir()

    with mock.patch("services.service_pmu.OUTPUT_DIR", str(tmp_path) + "/"), \
         mock.patch("services.service_pmu.requests.get", return_value=_fake_response(payload)) as mock_get:
        fetch_participants_pmu(1, 2, "2026-04-15")

    output_file = tmp_path / "participant" / "20260415_participant_r1_c2.json"
    assert output_file.exists()
    assert json.loads(output_file.read_text(encoding="utf-8")) == payload

    args, kwargs = mock_get.call_args
    assert args[0] == "https://online.turfinfo.api.pmu.fr/rest/client/61/programme/15042026/R1/C2/participants"
    assert kwargs["params"] == {"specialisation": "INTERNET"}
    assert kwargs["timeout"] == 10


# --- _get_reunions_courses ---

def test_get_reunions_courses_parses(tmp_path):
    course_dir = tmp_path / "course"
    course_dir.mkdir()
    programme = {
        "programme": {
            "reunions": [
                {"numOfficiel": 1, "courses": [{"numOrdre": 2}, {"numOrdre": 3}]},
                {"numOfficiel": 4, "courses": [{"numOrdre": 5}]},
            ]
        }
    }
    (course_dir / "20260415_course.json").write_text(json.dumps(programme), encoding="utf-8")

    with mock.patch("services.service_pmu.OUTPUT_DIR", str(tmp_path) + "/"):
        assert _get_reunions_courses("2026-04-15") == [(1, 2), (1, 3), (4, 5)]


# --- _get_data ---

def test_get_data_orchestrates(tmp_path):
    (tmp_path / "course").mkdir()
    (tmp_path / "participant").mkdir()

    with mock.patch("services.service_pmu.OUTPUT_DIR", str(tmp_path) + "/"), \
         mock.patch("services.service_pmu.fetch_course_pmu") as mock_fetch_course, \
         mock.patch("services.service_pmu._get_reunions_courses", return_value=[(1, 2), (1, 3)]), \
         mock.patch("services.service_pmu.fetch_participants_pmu") as mock_fetch_participants, \
         mock.patch("services.service_pmu.time.sleep"):
        _get_data("2026-04-15")

    mock_fetch_course.assert_called_once_with("2026-04-15")
    assert mock_fetch_participants.call_args_list == [
        mock.call(1, 2, "2026-04-15"),
        mock.call(1, 3, "2026-04-15"),
    ]


def test_get_data_uses_logical_date_when_param_empty(tmp_path):
    (tmp_path / "course").mkdir()
    (tmp_path / "participant").mkdir()

    with mock.patch("services.service_pmu.OUTPUT_DIR", str(tmp_path) + "/"), \
         mock.patch("services.service_pmu.fetch_course_pmu") as mock_fetch_course, \
         mock.patch("services.service_pmu._get_reunions_courses", return_value=[]), \
         mock.patch("services.service_pmu.fetch_participants_pmu"), \
         mock.patch("services.service_pmu.time.sleep"):
        _get_data("None", logical_date=datetime(2026, 4, 15))

    mock_fetch_course.assert_called_once_with("2026-04-15")
