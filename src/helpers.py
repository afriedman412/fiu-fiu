import os
from datetime import datetime as dt
from functools import wraps
from time import sleep
from typing import List, Tuple, Union

import pytz
import regex as re
import requests
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine

from .logger import logger

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
EMAILS_TO = ["david@readsludge.com", "donny@readsludge.com"] + [EMAIL_FROM]


def get_today() -> str:
    tz = pytz.timezone('America/New_York')
    today = dt.now().astimezone(tz)
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


def pp_query(
        url: str, offset: int = 0, retries=5, error_out: bool = True
        ) -> requests.Response:
    logger.debug(f"url: {url}\noffset: {offset}")
    counter = 0
    while counter < retries:
        r = requests.get(
            url=url,
            timeout=30,
            headers={"X-API-Key": os.environ['PRO_PUBLICA_API_KEY']},
            params={'offset': offset}
        )
        if r.status_code == 200 or not error_out:
            if '502 Bad Gateway' in r.content.decode():
                logger.debug(f"bad gateway ... retry {counter}/{retries}")
                counter += 1
                sleep(3)
                continue
            else:
                return r
        else:
            raise Exception(f"Bad Status Code: {r.status_code}, {r.content.decode()}")
    raise Exception(f"Bad Gateway, retries exceeded ({retries}).")


def check_for_daily_updates() -> bool:
    latest_date = query_table(f'select max(date) from {TABLE} limit 1')
    latest_date = latest_date[0][0]
    return os.getenv("TODAY") == latest_date


def send_email(
        subject,
        body,
        from_email: Union[str, list] = EMAIL_FROM,
        to_email: Union[str, list] = EMAILS_TO):
    message = Mail(
        from_email=from_email,
        to_emails=to_email,
        subject=subject,
        html_content=body)
    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        assert response.status_code == 202, f"Bad status code: {response.status_code}"
        logger.debug("Email sent successfully!")
        return True
    except Exception as e:
        logger.debug(f"Error while sending email: {e}")
        return False


def encode_df_url(value: str, url: str = None) -> str:
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


def sleep_after_execution(sleep_time):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            sleep(sleep_time)
            return result
        return wrapper
    return decorator
