import os
from datetime import datetime as dt
from time import sleep
from typing import List, Tuple, Union

import pandas as pd
import requests
from flask import Response, render_template, request
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


def get_committee_ie(committee_id: str):
    url = "https://api.propublica.org/campaign-finance/v1/{}/committees/{}/independent_expenditures.json"
    url = url.format(
        os.environ['CYCLE'],
        committee_id
    )
    results = []
    offset = 0
    while True:
        r = pp_query(url, offset)
        if r.status_code == 200 and r.json().get('results'):
            print(len(r.json().get('results')))
            results += r.json().get('results')
            offset += 20
        else:
            break
    return results


def check_for_daily_updates() -> bool:
    latest_date = query_table('select max(date) from fiu_pp limit 1')
    latest_date = latest_date[0][0]
    return dt.strftime(dt.today(), "%Y-%m-%d") == latest_date


def get_last_n_days(
        n: int = 1,
        fallback_n: int = 12
) -> pd.DataFrame:
    q = f"""
        SELECT *
        FROM fiu_pp
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {n} DAY)
        ORDER BY date DESC
        ;"""
    output = query_table(q)
    if output:
        return output
    else:
        q = f"""
            SELECT * FROM fiu_pp
            ORDER BY date DESC
            LIMIT {fallback_n}
            """
        output = query_table(q)
    return output


def get_new_ie_transactions():
    engine = make_conn()
    df = pd.read_sql("select distinct(unique_id) from fiu_pp", con=engine)
    url = "https://api.propublica.org/campaign-finance/v1/{}/independent_expenditures.json".format(
        os.environ['CYCLE']
    )
    offset = 0
    bucket = []
    while True:
        print(offset)
        r = pp_query(url, offset)
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
        new_transactions.to_sql("fiu_pp", con=engine, if_exists="append")
        return f"Successfully updated with {len(bucket)} new transactions."
    else:
        return "No new transactions."


def load_content(committee_id: Union[str, None] = None) -> str:
    if committee_id is None:
        new_transactions = check_for_daily_updates()
        today = dt.now().strftime("%Y-%m-%d")
        df = pd.DataFrame(get_last_n_days(0))
        df = df[DISPLAY_COLUMNS].sort_values(
            ['date', 'date_received'],
            ascending=False
        )
        filename = f"ie_{today}.csv"
        if request.method != "POST":
            df_html = df.to_html() if len(df) else None
            return render_template(
                'index.html',
                today=today,
                new_transactions=new_transactions,
                df_html=df_html
            )

    else:
        committee_ie = get_committee_ie(committee_id)
        df = pd.DataFrame(committee_ie)
        try:
            df = df[DISPLAY_COLUMNS].sort_values(
                ['date', 'date_received'],
                ascending=False
            )
        except KeyError:
            pass
        filename = f"{committee_id}_ie.csv"
        if request.method != "POST":
            df_html = df.to_html()
            return render_template(
                'committee_ie.html',
                committee_id=committee_id,
                df_html=df_html
            )

    return Response(
        df.to_csv(index=False),
        mimetype="text/csv",
        headers={
            "Content-disposition":
            "attachment; filename={}".format(filename)
        })
