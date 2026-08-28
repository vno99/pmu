import hashlib
import json
from pathlib import Path
from unittest import mock

import pytest
from psycopg2 import sql
from psycopg2.extras import Json

import dags.pmu_daily_insert_raw as d

DUMMY_SQL = "INSERT ..."


def _render_composed(composed):
    """Rendu SQL minimal sans connexion : remplace la vraie ``as_string``.

    psycopg2 ``Composable.as_string`` exige une vraie connexion (code C) pour
    quoter les identifiants. Ici on reconstitue la chaîne à partir des
    éléments du ``Composed`` (``sql.SQL`` / ``sql.Identifier``).
    """
    parts = []
    for el in composed.seq:
        if isinstance(el, sql.Identifier):
            # Identifier -> tuple des noms bruts via .strings
            parts.append(".".join('"%s"' % name for name in el.strings))
        elif isinstance(el, sql.Composed):
            parts.append(_render_composed(el))
        else:
            parts.append(el.string)
    return "".join(parts)


# --- build_insert_query ---

def test_build_insert_query_renders_valid_sql():
    with mock.patch.object(
        sql.Composed, "as_string", autospec=True,
        side_effect=lambda self, cursor: _render_composed(self),
    ):
        query = d.build_insert_query("raw_course", object())

    assert "INSERT INTO " in query
    assert '"raw"."raw_course"' in query
    assert "(source_file, file_hash, json_data)" in query
    assert "ON CONFLICT (source_file)" in query
    assert "EXCLUDED.json_data" in query
    assert "import_count = " in query
    assert '"raw"."raw_course".import_count + 1' in query


def test_build_insert_query_rejects_unknown_table():
    with pytest.raises(ValueError):
        d.build_insert_query("not_a_table", object())


# --- load_json_folder_to_raw ---

def _expected_hash(json_data):
    """Rejoue le calcul de hash du DAG : pop cached/timestampPMU puis dumps."""
    json_data = dict(json_data)
    json_data.pop("cached", None)
    json_data.pop("timestampPMU", None)
    normalized = json.dumps(json_data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


@pytest.fixture
def mock_hook():
    mock_hook = mock.MagicMock()
    # with hook.get_conn() as conn:  →  __enter__ -> conn
    mock_conn = mock_hook.get_conn.return_value.__enter__.return_value
    # with conn.cursor() as cur:     →  __enter__ -> cur
    mock_cur = mock_conn.cursor.return_value.__enter__.return_value
    with mock.patch.object(d, "PostgresHook", return_value=mock_hook):
        with mock.patch.object(d, "execute_values") as mock_exec, \
             mock.patch.object(d, "build_insert_query", return_value=DUMMY_SQL) as mock_build:
            yield mock_hook, mock_conn, mock_cur, mock_exec, mock_build


def test_load_json_folder_upserts_two_files(tmp_path, mock_hook):
    mock_hook, mock_conn, mock_cur, mock_exec, mock_build = mock_hook

    file_a = tmp_path / "20260415_a.json"
    file_b = tmp_path / "20260415_b.json"
    content_a = {"reunion": 1, "cached": True, "timestampPMU": 12345}
    content_b = {"reunion": 2, "timestampPMU": 67890}
    file_a.write_text(json.dumps(content_a), encoding="utf-8")
    file_b.write_text(json.dumps(content_b), encoding="utf-8")

    result = d.load_json_folder_to_raw("2026-04-15", tmp_path, "raw_course")

    assert result["files_seen"] == 2
    assert result["rows_attempted"] == 2
    assert result["failed_files"] == 0
    assert result["table_name"] == "raw_course"

    mock_hook.get_conn.assert_called_once()
    mock_build.assert_called_once_with("raw_course", mock_cur)

    args, kwargs = mock_exec.call_args
    assert args[0] is mock_cur
    assert args[1] == DUMMY_SQL
    assert kwargs["page_size"] == 1000

    rows = args[2]
    assert len(rows) == 2

    row_a, row_b = rows
    assert row_a[0] == str(file_a)
    assert row_a[1] == _expected_hash(content_a)
    assert row_a[2].adapted == {"reunion": 1}  # cached/timestampPMU supprimés

    assert row_b[0] == str(file_b)
    assert row_b[1] == _expected_hash(content_b)
    assert row_b[2].adapted == {"reunion": 2}


def test_load_json_folder_empty_no_connection(tmp_path, mock_hook):
    mock_hook, mock_conn, mock_cur, mock_exec, mock_build = mock_hook

    result = d.load_json_folder_to_raw("2026-04-15", tmp_path, "raw_course")

    assert result == {"files_seen": 0, "rows_attempted": 0, "failed_files": 0}
    mock_hook.get_conn.assert_not_called()
    mock_exec.assert_not_called()


def test_load_json_folder_invalid_json_counts_failure(tmp_path, mock_hook):
    mock_hook, mock_conn, mock_cur, mock_exec, mock_build = mock_hook

    (tmp_path / "20260415_bad.json").write_text("{ not valid json", encoding="utf-8")

    result = d.load_json_folder_to_raw("2026-04-15", tmp_path, "raw_course")

    assert result["files_seen"] == 1
    assert result["rows_attempted"] == 0
    assert result["failed_files"] == 1
    mock_exec.assert_not_called()
