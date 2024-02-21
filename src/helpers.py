from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
import os
import pandas as pd
import requests
from time import sleep
from datetime import datetime as dt
from typing import List, Tuple

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


def check_for_daily_updates() -> bool:
    latest_date = query_table('select max(date) from `fec-ie` limit 1')
    latest_date = latest_date[0][0]
    return dt.strftime(dt.today(), "%Y-%m-%d") == latest_date


def get_last_n_days(
        n: int = 1,
        fallback_n: int = 12
        ) -> pd.DataFrame:
    q = f"""
        SELECT *
        FROM `fec-ie`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {n} DAY)
        ORDER BY date DESC
        ;"""
    output = query_table(q)
    if output:
        return output
    else:
        q = f"""
            SELECT * FROM `fec-ie`
            ORDER BY date DESC
            LIMIT {fallback_n}
            """
        output = query_table(q)
    return output


def get_new_ie_transactions():
    engine = make_conn()
    df = pd.read_sql("select distinct(unique_id) from `fec-ie`", con=engine)
    url = "https://api.propublica.org/campaign-finance/v1/2024/independent_expenditures.json"
    offset = 0
    bucket = []
    while True:
        print(offset)
        r = requests.get(
            url=url,
            headers={"X-API-Key": os.environ['PRO_PUBLICA_API_KEY']},
            params={'offset': offset}
        )
        sleep(2)
        if r.status_code != 200:
            print(f"bad status code: {r.status_code}")
            break
        else:
            new_transactions = [
                r_ for r_ in r.json()['results']
                if r_['unique_id'] not in df['unique_id'].values
            ]
            if new_transactions:
                print(len(new_transactions))
                bucket += new_transactions
                offset += 20
            else:
                print("done!")
                break
    if len(bucket) > 0:
        new_transactions = pd.DataFrame(bucket)
        new_transactions.to_sql("fec-ie", con=engine, if_exists="append")
        return f"Successfully updated with {len(bucket)} new transactions."
    else:
        return "No new transactions."
