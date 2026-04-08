from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

default_args = {
    "owner": "airflow",
}

DBT_DIR = "--project-dir /opt/airflow/dbt/pmu --profiles-dir /opt/airflow/dbt/pmu"
STEPS = {
    "marts": "fct_course_participant dim_course dim_participant agg_driver agg_entraineur agg_hippodrome",
}


@dag(dag_id="pmu_dbt_int_to_mart",
     default_args=default_args, 
     catchup=False,
     tags=["pmu", "dbt"])
def pmu_dbt_int_to_mart():
    
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"dbt run {DBT_DIR} --select {STEPS['marts']}"
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"dbt test {DBT_DIR} --select {STEPS['marts']}"
    )

    dbt_run_marts >> dbt_test_marts

pmu_dbt_int_to_mart()