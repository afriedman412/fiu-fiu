import os

from flask import Flask, render_template, url_for

from .helpers import BASE_URL, CYCLE, get_today
from .src import get_new_ie_transactions, load_content


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


# @app.route('/date-check/<year>/<month>/<day>')
# def date_check(year: str, month: str, day: str) -> str:
#     url = os.path.join(BASE_URL, )
#     return


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
