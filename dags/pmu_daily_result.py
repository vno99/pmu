from datetime import datetime, timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task
from dateutil import tz

local_tz = tz.gettz("Europe/Paris")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 4, 1, tzinfo=local_tz),
}

@dag(dag_id="pmu_daily_result",
     description="Récupère quotidiennement les résultats PMU de la veille",
     default_args=default_args, 
     schedule="0 2 * * *",
     catchup=False,
     tags=["pmu", "dbt"],
     )
def pmu_daily_result():

    @task(task_id="calcule_date")
    def calcule_date(**context):
        return (context["logical_date"] - timedelta(days=1)).strftime("%d%m%Y")
    
    trigger_pmu_daily_call = TriggerDagRunOperator(
        task_id="trigger_pmu_daily_call",
        trigger_dag_id="pmu_daily_call",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        deferrable=True,
        conf={"current_date": "{{ ti.xcom_pull(task_ids='calcule_date') }}"}
    )

    calcule_date_task = calcule_date()

    calcule_date_task >> trigger_pmu_daily_call

pmu_daily_result()