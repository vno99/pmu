from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

default_args = {
    "owner": "airflow",
}

DBT_DIR = "--project-dir /opt/airflow/dbt/pmu --profiles-dir /opt/airflow/dbt/pmu"
STEPS = {
    "stg": "stg_raw__course stg_raw__participant",
    "int": "int_pmu__course int_pmu__participant",
}

@dag(dag_id="pmu_dbt_raw_to_intermediate",
     default_args=default_args, 
     catchup=False,
     tags=["pmu", "dbt"])
def pmu_dbt_raw_to_intermediate():
    
    dbt_run_stg = BashOperator(
        task_id="dbt_run_stg",
        bash_command=f"dbt run {DBT_DIR} --select {STEPS['stg']}"
    )

    dbt_test_stg = BashOperator(
        task_id="dbt_test_stg",
        bash_command=f"dbt test {DBT_DIR} --select {STEPS['stg']}"
    )

    dbt_run_int = BashOperator(
        task_id="dbt_run_int",
        bash_command=f"dbt run {DBT_DIR} --select {STEPS['int']}"
    )
    
    dbt_test_int = BashOperator(
        task_id="dbt_test_int",
        bash_command=f"dbt test {DBT_DIR} --select {STEPS['int']}"
    )

    dbt_run_stg >> dbt_test_stg >> dbt_run_int >> dbt_test_int

pmu_dbt_raw_to_intermediate()