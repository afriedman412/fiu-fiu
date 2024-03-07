import os

from flask import Flask, render_template, request, url_for

from .helpers import BASE_URL, CYCLE, get_today
from .src import (format_dates_output, get_data_by_date,
                  get_new_ie_transactions, load_content, get_daily_filings,
                  download_dates_output)


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


@app.route('/', methods=['GET', 'POST'])
def home() -> str:
    return load_content()


@app.route('/committee/<committee_id>', methods=['GET', 'POST'])
def committee_endpoint(committee_id: str) -> str:
    return load_content(committee_id)


@app.route('/basic', methods=['GET', 'POST'])
def basically() -> str:
    return "hello"


@app.route("/dates", methods=['GET', 'POST'])
def date_endpoint() -> str:
    if request.method == 'POST':
        if 'forms' in request.values:
            data = get_daily_filings(request.values.get('datepicker'))
            data = {k: v.to_html() for k, v in data.items()}
        else:
            checked = [k.replace("-", "_") for k in [
                'date',
                'date-received',
                'dissemination-date',
                'api'
            ] if request.values.get(k) == 'on']
            data = get_data_by_date(
                request.values.get('datepicker'),
                *checked
            )
            data = format_dates_output(data)
        if 'download' in request.values:
            download_dates_output(data)
            return
        else:
            return render_template(
                'dates.html',
                data=data
            )
    else:
        return render_template('dates.html')


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
