import hashlib
import json
import logging
import os
from datetime import datetime
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param, dag, task
from airflow.task.trigger_rule import TriggerRule
from psycopg2 import sql
from psycopg2.extras import Json, execute_values

from services.service_pmu import _get_dates

default_args = {
    "owner": "airflow",
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(console_handler)

DATA_DIR = Path("/data")
BATCH_SIZE = 2000
DB_CONN_ID = os.getenv("DB_CONN_ID", "data_db")

COURSE_DIR = Path("/data/pmu/course")
PARTICIPANT_DIR = Path("/data/pmu/participant")

COURSE_RAW_TABLE = "raw_course"
PARTICIPANT_RAW_TABLE = "raw_participant"

ALLOWED_TABLES = {COURSE_RAW_TABLE, PARTICIPANT_RAW_TABLE}


def build_insert_query(table_name: str, cursor, schema_name="raw") -> str:
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Table non autorisée: {table_name}")

    table_ident = sql.Identifier(schema_name, table_name)

    query = sql.SQL("""
        INSERT INTO {table} (source_file, file_hash, json_data)
        VALUES %s
        ON CONFLICT (source_file) 
        DO UPDATE SET 
            json_data = EXCLUDED.json_data,
            file_hash = EXCLUDED.file_hash,
            updated_at = NOW(),
            import_count = {table}.import_count + 1
    """).format(table=table_ident)

    return query.as_string(cursor)


def load_json_folder_to_raw(une_date, folder: Path, table_name: str) -> dict:
    _, date_filename = _get_dates(une_date)
    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)

    files_seen = 0
    rows_attempted = 0
    failed_files = 0

    json_files = sorted(folder.rglob(f"{date_filename}_*.json"))

    if not json_files:
        logger.warning(f"Aucun fichier trouvé pour {date_filename}")
        return {"files_seen": 0, "rows_attempted": 0, "failed_files": 0}

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            insert_sql = build_insert_query(table_name, cur)

            for i in range(0, len(json_files), BATCH_SIZE):
                batch_files = json_files[i:i + BATCH_SIZE]
                rows = []

                for file_path in batch_files:
                    files_seen += 1
                    try:
                        content = file_path.read_text(encoding="utf-8")
                        json_data = json.loads(content)
                        json_data.pop("cached", None)
                        json_data.pop("timestampPMU", None)
                        normalized_content = json.dumps(json_data, sort_keys=True, ensure_ascii=False)

                        file_hash = hashlib.sha256(normalized_content.encode("utf-8")).hexdigest()

                        rows.append(
                            (
                                str(file_path),
                                file_hash,
                                Json(json_data),
                            )
                        )

                        logger.info(f"Traitement de {str(file_path)}")

                    except Exception as e:
                        failed_files += 1
                        logger.exception(
                            "Erreur de lecture/parsing pour %s: %s",
                            file_path,
                            e,
                        )

                if not rows:
                    continue

                execute_values(cur, insert_sql, rows, page_size=1000)
                rows_attempted += len(rows)

    return {
        "table_name": table_name,
        "folder": str(folder),
        "files_seen": files_seen,
        "rows_attempted": rows_attempted,
        "failed_files": failed_files,
    }


@dag(dag_id="pmu_daily_insert_raw",
     description="Insère chaque jour les données PMU brutes dans la table raw PostgreSQL",
     default_args=default_args, 
     catchup=False,
     tags=["pmu", "dbt"],
     params={
         "current_date": Param(
             default=None,
             type=["string", "null"],
            description="Date des données à récupérer (DDMMYYYY)"
         )
     }
     )
def pmu_daily_insert_raw():
    
    @task(task_id="start")
    def start(**context):
        current_date = (
            context["params"].get("current_date")
            or context["dag_run"].conf.get("current_date")
            or context["logical_date"].strftime("%d%m%Y")
        )
        logger.info(f"Date résolue : {current_date}")
        return current_date

    @task(task_id="load_raw_courses")
    def load_raw_courses(a_date):
        logger.info(f"Traitement des courses pour {a_date}")
        return load_json_folder_to_raw(
            a_date,
            folder=COURSE_DIR,
            table_name=COURSE_RAW_TABLE
        )
    
    @task(task_id="load_raw_participants")
    def load_raw_participants(a_date):
        logger.info(f"Traitement des participants pour {a_date}")
        return load_json_folder_to_raw(
            a_date,
            folder=PARTICIPANT_DIR,
            table_name=PARTICIPANT_RAW_TABLE
        )
    
    @task(task_id="report_backfill", trigger_rule=TriggerRule.ALL_DONE)
    def report_backfill(course_stats: dict, participant_stats: dict):
        logger.info(f"Course stats: {course_stats}")
        logger.info(f"Participant stats: {participant_stats}")

    @task(task_id="watcher", trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def watcher():
        raise AirflowException("Une ou plusieurs tâches du DAG ont échoué.")

    start_task = start()
    course_stats = load_raw_courses(start_task)
    participant_stats = load_raw_participants(start_task)
    report_task = report_backfill(course_stats, participant_stats)
    watcher_task = watcher()

    start_task >> [course_stats, participant_stats]
    [start_task, course_stats, participant_stats, report_task] >> watcher_task

pmu_daily_insert_raw()
