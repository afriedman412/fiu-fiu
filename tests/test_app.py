import os

import pytest
from flask_testing import TestCase
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from app import app
from src.helpers import BASE_URL, EMAIL_FROM, make_conn, pp_query
from src.src import (get_committee_ie, get_daily_transactions,
                     get_data_by_date, send_email)


class TestFolio(TestCase):
    def create_app(self):
        return app

    def test_route(self):
        response = self.client.get("/basic")
        self.assert200(response)

    def test_endpoints(self):
        for endpoint in ["/", "/committee/C00864215", "/dates"]:
            endpoint_response = self.client.get(endpoint)
            self.assert200(endpoint_response)


def test_create_engine_success():
    engine = make_conn()
    try:
        with engine.connect() as conn:
            conn.execute(text("show tables"))
    except OperationalError as e:
        pytest.fail(f"Connection failed with error: {e}")


def test_create_engine_failure():
    engine = create_engine('sqlite:///this_will_fail')
    with pytest.raises(OperationalError):
        with engine.connect() as conn:
            conn.execute(text("show tables"))


def test_email():
    email_result = send_email("testing fiu email", "this is a test message for the fiu app", to_email=EMAIL_FROM)
    assert email_result


def test_pp_query():
    url = os.path.join(BASE_URL, "independent_expenditures.json")
    r = pp_query(url)
    assert r.status_code == 200


def test_committee_endpoint():
    r = get_committee_ie('C00799031')
    assert isinstance(r, list)
    assert len(r) > 0
    assert r[0]['fec_committee_name'] == 'United Democracy Project (Udp)'


def test_today_transactions():
    bucket = get_daily_transactions()
    assert isinstance(bucket, list)


def test_data_by_date():
    output = get_data_by_date("2024-02-27", 'date', 'date_received', 'dissemination_date', 'api')
    assert [k for k in output] == [
        'date', 'date_received', 'dissemination_date', 'api'
    ]
    assert [len(output[k]) for k in output] == [14, 29, 29, 20]
