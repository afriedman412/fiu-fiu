import os

import pytest
from flask_testing import TestCase
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from app import app
from src.helpers import BASE_URL, make_conn, pp_query
from src.src import get_today_transactions


class TestFolio(TestCase):
    def create_app(self):
        return app

    def test_route(self):
        response = self.client.get("/basic")
        self.assert200(response)

    def test_endpoints(self):
        home_response = self.client.get("/")
        self.assert200(home_response)
        committee_response = self.client.get("/committee/B00BS")
        self.assert200(committee_response)


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


def test_pp_query():
    url = os.path.join(BASE_URL, "independent_expenditures.json")
    r = pp_query(url)
    assert r.status_code == 200


def test_today_transactions():
    bucket = get_today_transactions()
    assert isinstance(bucket, list)
