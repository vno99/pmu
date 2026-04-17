import logging
import os
from datetime import datetime, timedelta

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param, dag

from services.service_pmu import _get_dates

from dateutil import tz

local_tz = tz.gettz("Europe/Paris")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 4, 1, tzinfo=local_tz),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

API_HOST = os.getenv("PMU_API_HOST", "http://host.docker.internal:7860/")
API_URL = os.getenv("PMU_API_URL", "predict")
DB_CONN_ID = os.getenv("DB_CONN_ID", "data_db")
TABLE_NAME = os.getenv("TABLE_NAME", "analytics_marts.dim_prediction")

logger = logging.getLogger(__name__)

def predict(current_date):
    api_url = f"{API_HOST}{API_URL}"
    current_date, _ = _get_dates(current_date)

    logger.info(f"Appel API pour la date: {current_date}")

    try:
        response = requests.post(
            api_url,
            json={
                "input": current_date
            },
            timeout=60
        )
        response.raise_for_status()
        
    except requests.exceptions.Timeout:
        logger.error(f"Timeout lors de l'appel API vers {api_url}")
        raise

    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur API: {e}")
        raise

    data = response.json()

    if not data:
        logger.error("Réponse API vide")
        raise ValueError("Aucune prédiction reçue de l'API")

    logger.info(f"Reception de {data["count"]} prévisions")


    return data

def save_predictions_to_db(ti):
    try:
        predictions_data = ti.xcom_pull(task_ids='api_call', key='return_value')
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des XCom: {e}")
        return

    if not predictions_data:
        logger.info("Aucune données retournées par l'API")
        return

    if not predictions_data.get("prediction"):
        logger.warning("Clé 'prediction' absente ou vide")
        return
    
    records = predictions_data["prediction"]
    if not records:
        logger.info("La liste des predictions est vide")
        return

    logger.info(f"Traitement de {predictions_data["count"]} previsions...")
    prediction_date = datetime.strptime(predictions_data["course_date"], '%d%m%Y').date()

    rows_to_insert = []
    for r in records:
        rows_to_insert.append(
            (
                r['race_id'], 
                r['horse_id'], 
                r['participant_num_pmu'], 
                r['pred_score'], 
                prediction_date,
                r['model_run'], 
            )
        )

    if not rows_to_insert:
        logger.warning("Aucune donnée valide à insérer")
        return


    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    
    target_fields = ['race_id', 'horse_id', 'participant_num_pmu', 'pred_score', 'prediction_date', 'model_run']

    hook.insert_rows(
        table=TABLE_NAME,
        rows=rows_to_insert,
        target_fields=target_fields,
        commit_every=1000,
        replace=True,
        replace_index=["race_id", "horse_id"],
    )

    logger.info(f"Insertion / Mise à jour réussie de {len(rows_to_insert)} predictions")


@dag(dag_id="pmu_daily_predict",
     default_args=default_args, 
     schedule="0 4 * * *",
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
def pmu_daily_predict():
    api_call = PythonOperator(
        task_id="api_call", 
        python_callable=predict, 
        retries=3, 
        retry_delay=timedelta(seconds=60), 
        execution_timeout=timedelta(seconds=300),
        op_kwargs={"current_date":  "{{ params.current_date }}"}
    )
    
    save_to_db = PythonOperator(
        task_id="save_predictions_to_db",
        python_callable=save_predictions_to_db,
    )

    api_call >> save_to_db

pmu_daily_predict()