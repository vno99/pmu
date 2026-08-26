"""Tests de l'API FastAPI (api/app.py).

Les variables d'environnement requises au top-level du module (MLFLOW_URI,
DB_URL, RUN_ID) sont posées AVANT l'import : ``os.environ[key]`` lève KeyError
sinon, et ``load_dotenv()`` ne surcharge pas une variable déjà définie.
Le lifespan (chargement MLflow) ne s'exécute pas à l'import, aucun serveur ni
connexion n'est donc nécessaire pour ces tests unitaires.
"""

import os

import pytest
from pydantic import ValidationError

os.environ.setdefault("MLFLOW_URI", "http://localhost:5000")
os.environ.setdefault("DB_URL", "postgresql://user:pass@localhost:5432/data_db")
os.environ.setdefault("RUN_ID", "test-run-id")

import api.app


# --- is_valid_ddmmyyyy ---

@pytest.mark.parametrize("valid", ["15042026", "01012000", "31122026"])
def test_is_valid_ddmmyyyy_accepts(valid):
    assert api.app.is_valid_ddmmyyyy(valid) is True


@pytest.mark.parametrize("invalid", [
    "20260415",   # format inversé (jour 20 / mois 26)
    "31022026",   # 31 février
    "1504202",    # trop court
    "150420266",  # trop long
    "abc",
    "",
])
def test_is_valid_ddmmyyyy_rejects(invalid):
    assert api.app.is_valid_ddmmyyyy(invalid) is False


# --- PredictRequest ---

def test_predict_request_valid():
    request = api.app.PredictRequest(input="15042026")
    assert request.input == "15042026"


@pytest.mark.parametrize("invalid", ["20260415", "aaaa", ""])
def test_predict_request_invalid(invalid):
    with pytest.raises(ValidationError):
        api.app.PredictRequest(input=invalid)
