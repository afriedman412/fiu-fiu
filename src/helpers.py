import os
from datetime import datetime as dt
from typing import List, Tuple

import pytz
import requests
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine

DISPLAY_COLUMNS = [
    'fec_committee_name',
    'candidate_name',
    'office',
    'state',
    'district',
    'amount',
    'date',
    'purpose',
    'payee',
    'date_received',
    'support_or_oppose',
]

CYCLE = "2024"
TABLE = "fiu_pp"
EMAIL_FROM = "afriedman412@gmail.com"
EMAIL_TO = "david@readsludge.com"


def get_today() -> str:
    tz = pytz.timezone('America/New_York')
    today = dt.utcnow().astimezone(tz)
    return today.strftime("%Y-%m-%d")


def query_table(q: str) -> List[Tuple]:
    engine = make_conn()
    with engine.connect() as conn:
        t = conn.execute(text(q))
        output = t.fetchall()
    engine.dispose()
    return output


def make_conn() -> Engine:
    sql_string = "mysql://{}:{}@{}/{}".format(
        "o1yiw20hxluaaf9p",
        os.getenv('MYSQL_PW'),
        "phtfaw4p6a970uc0.cbetxkdyhwsb.us-east-1.rds.amazonaws.com",
        "izeloqfyy070up9b"
    )
    engine = create_engine(sql_string)
    return engine


def pp_query(url: str, offset: int = 0) -> requests.Response:
    r = requests.get(
        url=url,
        headers={"X-API-Key": os.environ['PRO_PUBLICA_API_KEY']},
        params={'offset': offset}
    )
    return r


def check_for_daily_updates() -> bool:
    latest_date = query_table(f'select max(date) from {TABLE} limit 1')
    latest_date = latest_date[0][0]
    return os.getenv("TODAY") == latest_date


def send_email(subject, body):
    message = Mail(
        from_email=EMAIL_FROM,
        to_emails=[EMAIL_FROM, EMAIL_TO],
        subject=subject,
        html_content=body)
    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        assert response.status_code == 202, f"Bad status code: {response.status_code}"
        print("Email sent successfully!")
        return True
    except Exception as e:
        print("Error while sending email:", e)
        return False
