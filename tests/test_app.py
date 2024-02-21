import pytest
from flask_testing import TestCase
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from src.helpers import make_conn, pp_query
import os
from app import app


class TestFolio(TestCase):
    def create_app(self):
        return app

    def test_home_route(self):
        response = self.client.get("/basic")
        self.assert200(response)


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
    url = "https://api.propublica.org/campaign-finance/v1/{}/independent_expenditures.json".format(
        os.environ['CYCLE']
        )
    r = pp_query(url)
    assert r.status_code == 200
