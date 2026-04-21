from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag
from dateutil import tz

from services.service_pmu import _get_data

local_tz = tz.gettz("Europe/Paris")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 4, 1, tzinfo=local_tz),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(dag_id="pmu_daily_call",
     description="Exécute chaque jour l'appel à l'API PMU, " \
     "l'insertion en base PostgreSQL dans la couche raw, " \
     "puis les transformations dbt vers les couches intermediate et mart",
     default_args=default_args, 
     schedule="0 3 * * *",
     catchup=False,
     max_active_runs=1,
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

    api_call = PythonOperator(
        task_id="api_call", 
        python_callable=_get_data,
        retries=3, 
        retry_delay=timedelta(seconds=60), 
        execution_timeout=timedelta(seconds=300),
        op_kwargs={"current_date":  "{{ params.current_date }}"}
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

    pmu_dbt_raw_to_intermediate = TriggerDagRunOperator(
        task_id="pmu_dbt_raw_to_intermediate",
        trigger_dag_id="pmu_dbt_raw_to_intermediate",
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
        conf={"current_date": "{{ params.current_date }}"}
    )

    api_call >> pmu_daily_insert_raw >> pmu_dbt_raw_to_intermediate >> pmu_dbt_int_to_mart

pmu_daily_call()