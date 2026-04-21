from datetime import datetime

import pytest

from services.service_pmu import _get_dates

STR_DATE_FORMAT_OUTPUT = "%d%m%Y"
FILE_DATE_FORMAT_OUTPUT = "%Y%m%d"
NOW = datetime.now()
now_str_date = NOW.strftime(STR_DATE_FORMAT_OUTPUT)
now_date_filename = NOW.strftime(FILE_DATE_FORMAT_OUTPUT)

test_get_dates_data = [
    (None, now_str_date, now_date_filename),
    ("", now_str_date, now_date_filename),
    ("15042026", "15042026", "20260415"),
    ("2026", now_str_date, now_date_filename),
    ("20260415", now_str_date, now_date_filename),
]

@pytest.mark.parametrize("une_date, expected_str_date, expected_date_filename", test_get_dates_data)
def test_get_dates(une_date, expected_str_date, expected_date_filename):
    str_date, date_filename = _get_dates(une_date)

    assert str_date == expected_str_date
    assert date_filename == expected_date_filename