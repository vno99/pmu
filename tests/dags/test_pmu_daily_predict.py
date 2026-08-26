from datetime import date
from unittest import mock

import pytest
import requests
from airflow.exceptions import AirflowSkipException

import dags.pmu_daily_predict as d


def _fake_response(payload, status_error=None):
    """Réponse requests factice : raise_for_status() no-op (ou lève) et json() contrôlé."""
    resp = mock.Mock()
    if status_error is not None:
        resp.raise_for_status.side_effect = status_error
    else:
        resp.raise_for_status.return_value = None
    resp.json.return_value = payload
    return resp


def _expected_api_url():
    return f"{d.API_HOST}{d.API_URL}"


# --- predict ---

def test_predict_success():
    payload = {"count": 1, "course_date": "15042026", "prediction": [{"race_id": 1}]}
    with mock.patch("dags.pmu_daily_predict.requests.post",
                    return_value=_fake_response(payload)) as mock_post:
        result = d.predict("15042026")

    assert result == payload
    mock_post.assert_called_once_with(
        _expected_api_url(),
        json={"input": "15042026"},
        timeout=60,
    )


def test_predict_timeout_raises():
    with mock.patch("dags.pmu_daily_predict.requests.post",
                    side_effect=requests.exceptions.Timeout()):
        with pytest.raises(requests.exceptions.Timeout):
            d.predict("15042026")


def test_predict_http_error_raises():
    with mock.patch("dags.pmu_daily_predict.requests.post",
                    side_effect=requests.exceptions.HTTPError("boom")):
        with pytest.raises(requests.exceptions.HTTPError):
            d.predict("15042026")


@pytest.mark.parametrize("payload", [None, {}, {"prediction": []}])
def test_predict_empty_response_skips(payload):
    with mock.patch("dags.pmu_daily_predict.requests.post",
                    return_value=_fake_response(payload)):
        with pytest.raises(AirflowSkipException):
            d.predict("15042026")


# --- save_predictions_to_db ---

def _success_predictions():
    return {
        "count": 2,
        "course_date": "15042026",
        "prediction": [
            {"race_id": 1, "horse_id": 5, "participant_num_pmu": 3, "pred_score": 0.9, "model_run": "run-1"},
            {"race_id": 1, "horse_id": 6, "participant_num_pmu": 4, "pred_score": 0.8, "model_run": "run-1"},
        ],
    }


def test_save_predictions_to_db_inserts():
    hook_instance = mock.MagicMock()
    with mock.patch.object(d, "PostgresHook", return_value=hook_instance) as mock_hook_cls:
        ti = mock.Mock()
        ti.xcom_pull.return_value = _success_predictions()
        d.save_predictions_to_db(ti)

    mock_hook_cls.assert_called_once_with(postgres_conn_id=d.DB_CONN_ID)
    hook_instance.insert_rows.assert_called_once()

    kwargs = hook_instance.insert_rows.call_args.kwargs
    assert kwargs["table"] == d.TABLE_NAME
    assert kwargs["commit_every"] == 1000
    assert kwargs["replace"] is True
    assert kwargs["replace_index"] == ["race_id", "horse_id"]
    assert kwargs["target_fields"] == [
        "race_id", "horse_id", "participant_num_pmu", "pred_score", "prediction_date", "model_run",
    ]
    assert kwargs["rows"] == [
        (1, 5, 3, 0.9, date(2026, 4, 15), "run-1"),
        (1, 6, 4, 0.8, date(2026, 4, 15), "run-1"),
    ]


@pytest.mark.parametrize("xcom_value", [None, {"count": 0}, {"prediction": []}, {"prediction": [], "count": 0}])
def test_save_predictions_to_db_no_insert(xcom_value):
    hook_instance = mock.MagicMock()
    with mock.patch.object(d, "PostgresHook", return_value=hook_instance):
        ti = mock.Mock()
        ti.xcom_pull.return_value = xcom_value
        d.save_predictions_to_db(ti)

    hook_instance.insert_rows.assert_not_called()


def test_save_predictions_to_db_xcom_error_swallowed():
    hook_instance = mock.MagicMock()
    with mock.patch.object(d, "PostgresHook", return_value=hook_instance):
        ti = mock.Mock()
        ti.xcom_pull.side_effect = RuntimeError("boom")
        d.save_predictions_to_db(ti)

    hook_instance.insert_rows.assert_not_called()
