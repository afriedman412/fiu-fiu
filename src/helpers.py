import os
from datetime import datetime as dt
from typing import List, Tuple

import pytz
import requests
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
import re

DATA_COLUMNS = [
    'fec_committee_name',
    'fec_committee_id',
    'candidate_name',
    'office',
    'state',
    'district',
    'amount',
    'date',
    'date_received',
    'dissemination_date',
    'purpose',
    'payee',
    'support_or_oppose',
    'transaction_id'
]

COLUMNS_TO_CONVERT = [
    'expenditures_link',
    'transactions_link',
    'fec_uri',
    'original_uri',
    ('fec_committee_id', "committee/{}"),
    ('committee_id', "committee/{}")
]

DT_FORMAT = "%Y-%m-%d"
CYCLE = "2024"
BASE_URL = "https://api.propublica.org/campaign-finance/v1/{}/".format(CYCLE)
TABLE = "fiu_pp"
EMAIL_FROM = "afriedman412@gmail.com"
EMAILS_TO = ["david@readsludge.com", "donny@readsludge.com"]


def get_today() -> str:
    tz = pytz.timezone('America/New_York')
    today = dt.utcnow().astimezone(tz)
    today = today.strftime(DT_FORMAT)
    return today


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
    if r.status_code == 200:
        return r
    else:
        raise Exception(f"Bad Status Code: {r.status_code}, {r.content}")


def check_for_daily_updates() -> bool:
    latest_date = query_table(f'select max(date) from {TABLE} limit 1')
    latest_date = latest_date[0][0]
    return os.getenv("TODAY") == latest_date


def send_email(subject, body):
    message = Mail(
        from_email=EMAIL_FROM,
        to_emails=EMAILS_TO + [EMAIL_FROM],
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


def encode_df_url(value: str, url: str=None) -> str:
    if url:
        url = url.format(*[value] * url.count("{}"))
    else:
        url = value
    if value not in [None, 'None']:
        return f"<a href='{url}'>{value}</a>"
    else:
        return value


def encode_df_urls(df, column_info=COLUMNS_TO_CONVERT):
    for column in column_info:
        if isinstance(column, tuple):
            column, url = column
        else:
            url = None
        if column in df:
            if url:
                df[column] = df[column].map(lambda r: encode_df_url(r, url))
            else:
                df[column] = df[column].map(lambda r: encode_df_url(r))
    return df



def soup_to_dict_helper(regex: str, text: str) -> str:
    t = re.search(f"({regex})(.+)", text)
    try:
        return t.group(2)
    except AttributeError:
        return None
