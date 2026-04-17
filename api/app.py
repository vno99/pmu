import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, field_validator
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()

description = """
Ensemble des endpoints de l'API PMU
"""
tags_metadata = [
    {
        "name": "PMU API Endpoints",
        "description": "PMU API endpoints",
    }
]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(console_handler)

os.environ["MLFLOW_RECORD_ENV_VARS_IN_MODEL_LOGGING"] = "false"
MLFLOW_URI = os.environ["MLFLOW_URI"]
DB_URL = os.environ["DB_URL"]
EXPERIMENT_NAME = "pmu"
RUN_ID = os.environ["RUN_ID"]

SQL = """
select *
from analytics_marts.feature_store_horse_ranking_v3
where course_date = to_date(%(course_date)s, 'DDMMYYYY')
  and race_id is not null
  and horse_id is not null
order by course_date, course_heure_depart_ts, race_id, horse_id
"""

@asynccontextmanager
async def lifespan(app: FastAPI):
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    run = mlflow.get_run(RUN_ID)
    if not run:
        raise ValueError(f"Run MLFlow inexistant : {RUN_ID}")

    app.state.model = mlflow.lightgbm.load_model(f"runs:/{RUN_ID}/ranker_v5_hybrid")
    model_features_uri = f"runs:/{RUN_ID}/metadata/model_features.json"
    app.state.metadata = mlflow.artifacts.load_dict(model_features_uri)
    app.state.engine = create_engine(DB_URL)

    yield

    app.state.engine.dispose()

app = FastAPI(
    title="PMU API Endpoints",
    description=description,
    version="0.1",
    openapi_tags=tags_metadata,
    lifespan=lifespan,

)

def is_valid_ddmmyyyy(date_str):
    try:
        parsed = datetime.strptime(date_str, "%d%m%Y")
        # Reformate et compare à l'original
        return parsed.strftime("%d%m%Y") == date_str
    except ValueError:
        return False
    
class PredictRequest(BaseModel):
    input: str

    @field_validator('input')
    @classmethod
    def validate_date(cls, value):
        if not is_valid_ddmmyyyy(value):
            raise ValueError("Format attendu: DDMMYYYY (ex: 15042026)")
        return value

    model_config = {
        "json_schema_extra": {
            "example": {
                "input": "15042026"
            }
        }
    }
    

@app.get("/", tags=["Endpoints"], response_class=HTMLResponse, include_in_schema=False)
async def root():
    message = "La documentation l'API est disponible a cette url :  <a href='docs'>`/docs`<a/>"
    return message


@app.post("/predict", tags=["Machine Learning"])
async def predict(request: PredictRequest):
    """
    Permet de prédire l'ordre d'arrivée des chevaux
    """
    try:
        engine = app.state.engine
        df_selected_course = pd.read_sql(SQL, engine, params={"course_date": request.input})

    except SQLAlchemyError as e:
        logger.error(f"Erreur SQL: {e}")
        return {"error": "Erreur de base de données", "message": str(e)}


    logger.info(f"Requête SQL: {request.input}")
    logger.debug(f"Nombre de lignes : {len(df_selected_course)}")

    if df_selected_course.empty:
        return {"prediction": [], "message": "Aucune donnée trouvée"}

    model = app.state.model
    metadata = app.state.metadata

    feature_cols = metadata["feature_cols"]
    categorical_cols = metadata["categorical_cols"]

    for col in feature_cols:
        if col not in df_selected_course.columns:
            df_selected_course[col] = np.nan 

        if col in categorical_cols:
            df_selected_course[col] = df_selected_course[col].astype("category")
        else:
            df_selected_course[col] = pd.to_numeric(df_selected_course[col], errors="coerce")
    
    X_selected_course = df_selected_course[feature_cols]

    df_selected_course["pred_score"] = model.predict(X_selected_course)

    pred_result = df_selected_course[["race_id", "horse_id", "participant_num_pmu", "pred_score"]].sort_values(by=["race_id", "pred_score"], ascending=[True, False], na_position="last")
    pred_result["model_run"] = RUN_ID

    # selected_date = pred_result.iloc[0]["race_id"][:10] if not pred_result.empty else None

    # pred_result.to_csv(f"predi_{selected_date}.csv")

    now_utc = datetime.now(timezone.utc)
    iso_string = now_utc.isoformat()

    return {
        "prediction": pred_result.to_dict(orient="records"),
        "count": len(pred_result),
        "timestamp": iso_string,
        "course_date": request.input
    }

