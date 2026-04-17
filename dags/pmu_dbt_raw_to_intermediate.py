import logging

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task

from services.service_pmu import _get_dates

default_args = {
    "owner": "airflow",
}

DBT_DIR = "--project-dir /opt/airflow/dbt/pmu --profiles-dir /opt/airflow/dbt/pmu"
STEPS = {
    "stg": "stg_raw__course stg_raw__participant",
    "int": "int_pmu__course int_pmu__participant",
}

@dag(dag_id="pmu_dbt_raw_to_intermediate",
     description="Exécute les modèles dbt de staging puis d'intermediate, avec les tests associés, à partir des données PMU de la couche raw",
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
def pmu_dbt_raw_to_intermediate():
    
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
        bash_command=(f"dbt run {DBT_DIR} --select {STEPS['int']} --vars '{dbt_vars}' "
                      + "{% if dag_run.conf.get('full_refresh') == 'true' %}--full-refresh{% endif %}")
    )
    
    dbt_test_int = BashOperator(
        task_id="dbt_test_int",
        bash_command=f"dbt test {DBT_DIR} --select {STEPS['int']}"
    )

    start_task = start()

    start_task >> dbt_run_stg >> dbt_test_stg >> dbt_run_int >> dbt_test_int

pmu_dbt_raw_to_intermediate()