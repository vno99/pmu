import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from airflow.task.trigger_rule import TriggerRule
from psycopg2 import sql
from psycopg2.extras import Json, execute_values

default_args = {
    "owner": "airflow",
}

DATA_DIR = Path("/data")
BATCH_SIZE = 2000
POSTGRES_CONN_ID = "data_db"

COURSE_DIR = Path("/data/pmu/course")
PARTICIPANT_DIR = Path("/data/pmu/participant")

COURSE_RAW_TABLE = "raw_course"
PARTICIPANT_RAW_TABLE = "raw_participant"

ALLOWED_TABLES = {COURSE_RAW_TABLE, PARTICIPANT_RAW_TABLE}

def build_insert_query(table_name: str, cursor, schema_name="raw") -> str:
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Table non autorisée: {table_name}")

    query = sql.SQL("""
        INSERT INTO {} (source_file, file_hash, json_data)
        VALUES %s
        ON CONFLICT (file_hash) DO NOTHING
    """).format(sql.Identifier(schema_name, table_name))

    return query.as_string(cursor)

def load_json_folder_to_raw(folder: Path, table_name: str) -> dict:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()

    files_seen = 0
    rows_attempted = 0
    failed_files = 0

    json_files = sorted(folder.rglob("*.json"))

    try:
        with conn:
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
                            file_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

                            rows.append(
                                (
                                    str(file_path),
                                    file_hash,
                                    Json(json_data),
                                )
                            )

                            logging.info(f"Traitement de {str(file_path)}")

                        except Exception as e:
                            failed_files += 1
                            logging.exception(
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

    finally:
        conn.close()

@dag(dag_id="pmu_full_insert_raw",
     default_args=default_args, 
     catchup=False,
     tags=["pmu", "dbt"])
def pmu_full_insert_raw():
    
    @task(task_id="start")
    def start():
        return "start"

    @task(task_id="load_raw_courses")
    def load_raw_courses():
        return load_json_folder_to_raw(
            folder=COURSE_DIR,
            table_name=COURSE_RAW_TABLE,
        )
    
    @task(task_id="load_raw_participants")
    def load_raw_participants():
        return load_json_folder_to_raw(
            folder=PARTICIPANT_DIR,
            table_name=PARTICIPANT_RAW_TABLE,
        )
    
    @task(task_id="report_backfill", trigger_rule=TriggerRule.ALL_DONE)
    def report_backfill(course_stats: dict, participant_stats: dict):
        logging.info(f"Course stats: {course_stats}")
        logging.info(f"Participant stats: {participant_stats}")

    start_task = start()
    course_stats = load_raw_courses()
    participant_stats = load_raw_participants()
    report_task = report_backfill(course_stats, participant_stats)

    start_task >> [course_stats, participant_stats] >> report_task

pmu_full_insert_raw()
