import os

from flask import Flask, url_for

from .helpers import CYCLE, load_content


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
def committee_endpoint(committee_id: str) -> str:
    return load_content(committee_id)


@app.route('/basic', methods=['GET', 'POST'])
def query() -> str:
    return "hello"
