from flask import Flask, render_template, Response, url_for, request
import os
import pandas as pd
from .helpers import (
    check_for_daily_updates,
    get_new_ie_transactions,
    get_last_n_days,
    get_committee_ie,
    load_content,
    DISPLAY_COLUMNS,
    CYCLE
    )
from datetime import datetime as dt


def generate_app() -> Flask:
    app = Flask(__name__)
    os.environ['CYCLE'] = CYCLE
    if not os.getenv("PRO_PUBLICA_API_KEY"):
        from dotenv import load_dotenv
        load_dotenv()
    assert os.getenv("PRO_PUBLICA_API_KEY") is not None
    return app


app = generate_app()


@app.route("/favicon.ico")
def favicon():
    return url_for('static', filename='data:,')


@app.route('/', methods=['GET', 'POST'])
def home() -> str:
    return load_content()


@app.route('/committee/<committee_id>', methods=['GET', 'POST'])
def committee_endpoint(committee_id: str):
    return load_content(committee_id)


@app.route('/basic', methods=['GET', 'POST'])
def query():
    return "hello"
