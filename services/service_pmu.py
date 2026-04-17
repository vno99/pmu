import json
import logging
import time
from datetime import datetime, timedelta

import requests

logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

API_HOST = "https://online.turfinfo.api.pmu.fr/"
API_URL = "rest/client/61/programme/"
LISTE_PARTICIPANTS_COURSE = ""

OUTPUT_DIR = "/data/pmu/"

def _get_dates(une_date):
    str_date = (
        datetime.now().strftime("%d%m%Y") 
        if une_date in (None, "None", "null", "") 
        else une_date.strftime("%d%m%Y") 
        if isinstance(une_date, datetime) 
        else une_date
    )

    day = str_date[:2]
    month = str_date[2:4]
    year = str_date[4:]

    date_filename = f"{year}{month}{day}"

    return (str_date, date_filename)


def fetch_course_pmu(une_date):
    str_date, date_filename = _get_dates(une_date)
    api_url = f"{API_HOST}{API_URL}{str_date}"
    filename = f"{OUTPUT_DIR}course/{date_filename}_course.json"

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
    str_date, date_filename = _get_dates(une_date)
    api_url = f"{API_HOST}{API_URL}{str_date}/R{num_reunion}/C{num_course}/participants"
    filename = f"{OUTPUT_DIR}participant/{date_filename}_participant_r{num_reunion}_c{num_course}.json"

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
    _, date_filename = _get_dates(une_date)
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
    start_date = datetime.strptime(start_date_var, "%d%m%Y")
    end_date = datetime.today()

    current_date = start_date

    while current_date <= end_date:
        # str_date, date_filename = _get_dates(current_date)
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
        

def _get_data(current_date: str, **context):
    if current_date in ["None", "null", ""]:
        current_date = context["logical_date"].strftime("%d%m%Y")

    fetch_course_pmu(current_date)
    time.sleep(0.15)

    reunions_courses = _get_reunions_courses(current_date)

    for a_reunion_course in reunions_courses:
        num_reunion, num_course = a_reunion_course

        fetch_participants_pmu(num_reunion, num_course, current_date)
        time.sleep(0.15)


# if __name__ == "__main__":
#     _get_full_data_from("14082025")