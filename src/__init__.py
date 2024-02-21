from flask import Flask, render_template, Response, url_for
import os
import pandas as pd
from .helpers import (
    check_for_daily_updates,
    get_new_ie_transactions,
    get_last_n_days,
    DISPLAY_COLUMNS
    )
from datetime import datetime as dt


def generate_app() -> Flask:
    app = Flask(__name__)
    if not os.getenv("PRO_PUBLICA_API_KEY"):
        from dotenv import load_dotenv
        load_dotenv()
    assert os.getenv("PRO_PUBLICA_API_KEY") is not None
    return app


app = generate_app()

app.route("/update")(get_new_ie_transactions)


@app.route("/favicon.ico")
def favicon():
    return url_for('static', filename='data:,')


@app.route('/')
def home() -> str:
    new_transactions = check_for_daily_updates()
    today = dt.now().strftime("%Y-%m-%d")
    df = pd.DataFrame(get_last_n_days(0))
    df = df[DISPLAY_COLUMNS].sort_values(
            ['date', 'date_received'],
            ascending=False
        )
    df_html = df.to_html()
    return render_template(
        'index.html',
        today=today,
        new_transactions=new_transactions,
        df_html=df_html
        )


@app.route('/basic', methods=['GET', 'POST'])
def query():
    return "hello"


@app.route('/download_csv', methods=['GET', 'POST'])
def download_csv():
    df = pd.DataFrame(
        get_last_n_days(0)
        ).sort_values(
            ['date', 'date_received'],
            ascending=False
    )
    return Response(
        df.to_csv(index=False),
        mimetype="text/csv",
        headers={"Content-disposition":
                 "attachment; filename=data.csv"})
