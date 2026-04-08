from datetime import datetime

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param, dag
from pendulum import timezone

from services.api_pmu import _get_data

local_tz = timezone("Europe/Paris")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 4, 1, tzinfo=local_tz),
}

@dag(dag_id="pmu_daily_call",
     default_args=default_args, 
     schedule="0 3 * * *",
     catchup=False,
     tags=["pmu", "dbt"],
     params={
         "current_date": Param(
            default=None,
            type=["string", "null"],
            description="Date (DDMMYYYY). Si vide, utilise la date du DAG run."
        )
     }
     )
def pmu_daily_call():

    api_call = PythonOperator(task_id="api_call", python_callable=_get_data,
                       op_kwargs={
                           "current_date":  "{{ params.current_date }}"
                           },
                       )
    
    pmu_daily_insert_raw = TriggerDagRunOperator(
        task_id="pmu_daily_insert_raw",
        trigger_dag_id="pmu_daily_insert_raw",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        deferrable=True,
        conf={"current_date": "{{ params.current_date }}"}
    )

    pmu_dbt_int_to_mart = TriggerDagRunOperator(
        task_id="pmu_dbt_int_to_mart",
        trigger_dag_id="pmu_dbt_int_to_mart",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        deferrable=True,
    )

    api_call >> pmu_daily_insert_raw >> pmu_dbt_int_to_mart

pmu_daily_call()