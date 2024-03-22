import os

import pandas as pd
from flask import Flask, jsonify, redirect, render_template, request, url_for

from .helpers import BASE_URL, CYCLE, get_today
from .src import (download_dates_output, format_dates_output,
                  get_daily_24_48_forms, get_data_by_date,
                  get_new_ie_transactions, load_transactions, parse_24_48)


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

form_urls = []

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
    return load_transactions()


@app.route('/committee')
def reroute_committee():
    """I added this after I baked the code to load comittee info into load_transactions()"""
    committee_id = request.args.get("committee_id")
    if committee_id:
        return redirect(f"/committee/{committee_id}")
    else:
        return render_template("committee_ie.html")


@app.route('/committee/<committee_id>', methods=['GET', 'POST'])
def committee_endpoint(committee_id: str) -> str:
    return load_transactions(committee_id)


@app.route('/basic', methods=['GET', 'POST'])
def basically() -> str:
    return "hello"


@app.route("/dates", methods=['GET', 'POST'])
def date_endpoint() -> str:
    """
    Returns blank "dates.html" page if method is GET.

    If method is POST:
    - use 'datepicker' from POST data to get the date to use.
    - loads 24/48 Hour Filings if 'forms' is in request.values
    (this is used to find 24/48 hour filings that haven't shown up in the rest of the data yet)
    - otherwise loads forms for selected dates/date categories
    (use of 'date' columns is inconsistent, this allows some parsing)

    If 'download' is in request.values, download current data as .csv.

    Jumping through some hoops to nail down query errors and response codes and make formats align for downstream processing.
    """
    message = None
    if request.method == 'POST':
        date = request.values.get('datepicker')
        if request.values['forms'] == 'on':
            #  this is unruly!
            data = get_daily_24_48_forms(date)
            if isinstance(data.get("24- and 48- Hour Filings"), str):
                message = data.get("24- and 48- Hour Filings")
            else:
                global form_urls
                form_urls = data[
                    "24- and 48- Hour Filings"
                ]['fec_uri'].to_list()  # save form urls for later
                print("Form urls ...", len(form_urls))
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
                date=date,
                message=message
            )
    else:
        return render_template(
            'dates.html',
            date=os.environ['TODAY']
        )


@app.route('/form-url-check')
def show_form_urls() -> str:
    global form_urls
    try:
        return jsonify(form_urls.to_json())
    except AttributeError:
        return jsonify(form_urls)


@app.route('/forms/<date>')
def expand_forms(date) -> str:
    split_results = {}
    global form_urls
    form_scrapes = []
    for url in form_urls:
        scrape_data = parse_24_48(url)
        form_scrapes.append(scrape_data)
        split_results = {
            f'{k} HOUR NOTICE:': pd.DataFrame(
                [r for r in form_scrapes if r['form_type'] == k]
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


@app.route('/progress/<n>')
def progress_bar_test(n: int) -> str:

    return
