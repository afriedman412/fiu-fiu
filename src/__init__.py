import os

from flask import Flask, render_template, request, url_for, redirect, jsonify
import pandas as pd

from .helpers import BASE_URL, CYCLE, get_today
from .src import (format_dates_output, get_data_by_date,
                  get_new_ie_transactions, load_content, get_daily_filings,
                  download_dates_output, parse_24_48)


def generate_app() -> Flask:
    app = Flask(__name__)
    os.environ['CYCLE'] = CYCLE
    os.environ['TODAY'] = get_today()
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


@app.route("/date-check")
def check_date() -> str:
    return f"""
    TODAY set to {os.getenv('TODAY', 'set_error')}
    """


@app.route('/')
def home() -> str:
    return load_content()


@app.route('/committee')
def reroute_committee():
    """I added this after I baked the code to load comittee info into load_content()"""
    committee_id = request.args.get("committee_id")
    if committee_id:
        return redirect(f"/committee/{committee_id}")
    else:
        return render_template("committee_ie.html")


@app.route('/committee/<committee_id>', methods=['GET', 'POST'])
def committee_endpoint(committee_id: str) -> str:
    return load_content(committee_id)


@app.route('/basic', methods=['GET', 'POST'])
def basically() -> str:
    return "hello"


@app.route("/dates", methods=['GET', 'POST'])
def date_endpoint() -> str:
    if request.method == 'POST':
        date = request.values.get('datepicker')
        if 'forms' in request.values:
            data = get_daily_filings(date)
            data = format_dates_output(data, None)
        else:
            checked = [k.replace("-", "_") for k in [
                'date',
                'date-received',
                'dissemination-date',
                'api'
            ] if request.values.get(k) == 'on']
            data = get_data_by_date(
                date,
                *checked
            )
            data = format_dates_output(data)
        if 'download' in request.values:
            download_dates_output(data)
            return
        else:
            return render_template(
                'dates.html',
                data=data,
                date=date
            )
    else:
        return render_template(
            'dates.html',
            date=os.environ['TODAY']
            )
    

@app.route('/forms/<date>')
def expand_forms(date) -> str:
    data = get_daily_filings(date)
    results = [parse_24_48(url) for url in data['24- and 48- Hour Filings']['fec_uri']]
    split_results = {
        f'{k} HOUR NOTICE:': pd.DataFrame(
            [r for r in results if r['form_type']==k]
            )
        for k in ['24', '48']
    }
    split_results = format_dates_output(split_results, COLUMNS=None)
    return render_template(
        'dates.html',
        data=split_results,
        date=date
        )



@app.route('/routes', methods=['GET'])
def get_routes() -> str:
    # Create available routes UI on home page.
    routes = []
    for rule in app.url_map.iter_rules():
        routes.append({
            "endpoint": rule.endpoint,
            "methods": list(rule.methods),
            "url": str(rule)
        })
    return render_template('routes.html', routes=routes)
