import logging
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task

from services.api_pmu import _get_dates

default_args = {
    "owner": "airflow",
}

DBT_DIR = "--project-dir /opt/airflow/dbt/pmu --profiles-dir /opt/airflow/dbt/pmu"
STEPS = {
    "marts": "fct_course_participant dim_course dim_participant agg_driver agg_entraineur agg_hippodrome",
}

# DEPRECATED
@dag(dag_id="pmu_daily_dbt_int_to_mart",
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
def pmu_daily_dbt_int_to_mart():

    @task(task_id="start")
    def start(**context):
        current_date = (
            context["params"].get("current_date")
            or context["dag_run"].conf.get("current_date")
            or context["logical_date"].strftime("%d%m%Y")
        )
        logging.info(f"Date résolue : {current_date}")

        _, date_filename = _get_dates(current_date)
        return {"current_date": date_filename}
    
    dbt_vars = "{{ ti.xcom_pull(task_ids='start') | tojson }}"

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"dbt run {DBT_DIR} --select {STEPS['marts']} --vars '{dbt_vars}'"
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"dbt test {DBT_DIR} --select {STEPS['marts']} --vars '{dbt_vars}'"
    )

    current_date = start()

    current_date >> dbt_run_marts >> dbt_test_marts

pmu_daily_dbt_int_to_mart()