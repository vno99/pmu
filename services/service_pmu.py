import json
import logging
import time
from datetime import date, datetime, timedelta

import requests

logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

API_HOST = "https://online.turfinfo.api.pmu.fr/"
API_URL = "rest/client/61/programme/"

OUTPUT_DIR = "/data/pmu/"

STR_DATE_FORMAT_OUTPUT = "%d%m%Y"
FILE_DATE_FORMAT_OUTPUT = "%Y%m%d"
ISO_DATE_FORMAT = "%Y-%m-%d"

def normalize_date(une_date) -> date:
    """Normalise une date d'entrée vers un ``datetime.date``.

    - ``None`` / ``""`` / ``"None"`` / ``"null"`` → date du jour (param de DAG vide).
    - ``datetime`` / ``date`` → sa composante date.
    - ``str`` → format ISO ``YYYY-MM-DD`` (ex : ``2026-04-15``).

    Lève ``ValueError`` pour toute autre entrée : plutôt qu'un repli silencieux
    sur la date du jour (qui masquait les erreurs de saisie), on échoue fort.
    """
    if une_date in (None, "", "None", "null"):
        return datetime.now().date()
    if isinstance(une_date, datetime):
        return une_date.date()
    if isinstance(une_date, date):
        return une_date
    if isinstance(une_date, str):
        try:
            parsed = datetime.strptime(une_date, ISO_DATE_FORMAT)
            is_strict = parsed.strftime(ISO_DATE_FORMAT) == une_date
        except (ValueError, TypeError):
            is_strict = False
        if not is_strict:
            logging.error(f"Format de date ISO invalide : {une_date!r}")
            raise ValueError(
                f"Format de date ISO attendu (YYYY-MM-DD), reçu : {une_date!r}"
            ) from None
        return parsed.date()
    logging.error(f"Type de date non supporté : {type(une_date).__name__}")
    raise ValueError(f"Type de date non supporté : {type(une_date).__name__}")


def _file_date(une_date) -> str:
    """Format YYYYMMDD utilisé dans les noms de fichiers JSON (contrat stockage)."""
    return normalize_date(une_date).strftime(FILE_DATE_FORMAT_OUTPUT)


def resolve_dag_date(une_date, fallback_date):
    """Résout la date d'un paramètre de DAG en filtrant les sentinelles vides.

    Airflow propage un paramètre de date vide sous forme de chaîne ``"None"``
    (rendu Jinja) à travers les ``conf`` des ``TriggerDagRunOperator``. Ces
    sentinelles doivent être traitées comme « date absente » et repliées sur
    ``fallback_date`` (typiquement le ``logical_date`` du run), plutôt que
    transmises telles quelles à dbt (qui échouerait sur ``'None'::date``).
    """
    if une_date in (None, "", "None", "null"):
        return fallback_date
    return une_date


def fetch_course_pmu(une_date):
    date_obj = normalize_date(une_date)
    api_url = f"{API_HOST}{API_URL}{date_obj.strftime(STR_DATE_FORMAT_OUTPUT)}"
    filename = f"{OUTPUT_DIR}course/{date_obj.strftime(FILE_DATE_FORMAT_OUTPUT)}_course.json"

    logging.info(f"fetch_course_pmu - request : {api_url}")

    response = requests.get(
        api_url,
        params={
            "meteo": True,
            "specialisation": "INTERNET"
        },
        timeout=10
    )

    response.raise_for_status()
    data = response.json()

    with open(filename, "w") as f:
        json.dump(data, f, indent=4)

    logging.info(f"fetch_course_pmu - write file : {filename}")


def fetch_participants_pmu(num_reunion, num_course, une_date):
    date_obj = normalize_date(une_date)
    api_url = f"{API_HOST}{API_URL}{date_obj.strftime(STR_DATE_FORMAT_OUTPUT)}/R{num_reunion}/C{num_course}/participants"
    filename = f"{OUTPUT_DIR}participant/{date_obj.strftime(FILE_DATE_FORMAT_OUTPUT)}_participant_r{num_reunion}_c{num_course}.json"

    logging.info(f"fetch_participants - request : {api_url}")

    response = requests.get(
        api_url,
        params={
            "specialisation": "INTERNET"
        },
        timeout=10
    )

    response.raise_for_status()
    data = response.json()
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)

    logging.info(f"fetch_participants - write file : {filename}")

def _get_reunions_courses(une_date):
    date_filename = _file_date(une_date)
    list_res = []

    with open(f"{OUTPUT_DIR}course/{date_filename}_course.json", "r") as f:
        data = json.load(f)

    programme = data.get("programme", {})
    reunions = programme.get("reunions", [])

    for reunion in reunions:
        num_reunion = reunion.get("numOfficiel")

        courses = reunion.get("courses", [])

        for course in courses:
            num_course = course.get("numOrdre")

            list_res.append((num_reunion, num_course))

    return list_res

    
def _get_full_data_from(start_date_var="09032013"):
    start_date = datetime.strptime(start_date_var, STR_DATE_FORMAT_OUTPUT)
    end_date = datetime.today()

    current_date = start_date

    while current_date <= end_date:
        fetch_course_pmu(current_date)
        time.sleep(0.15)

        reunions_courses = _get_reunions_courses(current_date)

        for a_reunion_course in reunions_courses:
            num_reunion, num_course = a_reunion_course

            fetch_participants_pmu(num_reunion, num_course, current_date)
            time.sleep(0.15)

        # print(current_date)

        current_date += timedelta(days=1)

        # time.sleep(0.5)
        

def _get_data(current_date=None, **context):
    if current_date in (None, "", "None", "null"):
        current_date = context["logical_date"].date().isoformat()

    fetch_course_pmu(current_date)
    time.sleep(0.15)

    reunions_courses = _get_reunions_courses(current_date)

    for a_reunion_course in reunions_courses:
        num_reunion, num_course = a_reunion_course

        fetch_participants_pmu(num_reunion, num_course, current_date)
        time.sleep(0.15)