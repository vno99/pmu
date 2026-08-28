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


# --- is_valid_iso_date ---

@pytest.mark.parametrize("valid", ["2026-04-15", "2000-01-01", "2026-12-31"])
def test_is_valid_iso_date_accepts(valid):
    assert api.app.is_valid_iso_date(valid) is True


@pytest.mark.parametrize("invalid", [
    "15042026",   # format inversé (jour 15 / mois 20)
    "20260415",   # sans tirets
    "2026-4-5",   # strptime accepte sans zéro → round-trip le rejette
    "2026-13-01", # 13e mois
    "2026-04-32", # 32 avril
    "abc",
    "",
])
def test_is_valid_iso_date_rejects(invalid):
    assert api.app.is_valid_iso_date(invalid) is False


# --- PredictRequest ---

def test_predict_request_valid():
    request = api.app.PredictRequest(input="2026-04-15")
    assert request.input == "2026-04-15"


@pytest.mark.parametrize("invalid", ["20260415", "15042026", "aaaa", ""])
def test_predict_request_invalid(invalid):
    with pytest.raises(ValidationError):
        api.app.PredictRequest(input=invalid)
