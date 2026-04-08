from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag

default_args = {
    "owner": "airflow",
}

POSTGRES_CONN_ID = "data_db"

@dag(dag_id="pmu_init_schema", 
     default_args=default_args, 
     catchup=False, 
     tags=["pmu", "dbt"],
     template_searchpath=["/opt/airflow/sql"])
def pmu_create_raw_tables():
    create_raw_tables = SQLExecuteQueryOperator(
        task_id="create_raw_tables",
        conn_id=POSTGRES_CONN_ID,
        sql="create_raw_tables.sql",
    )

pmu_create_raw_tables()