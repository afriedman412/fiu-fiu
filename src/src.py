import os
from time import sleep
from typing import Union

import pandas as pd
from flask import Response, render_template, request

from .helpers import (DISPLAY_COLUMNS, TABLE, check_for_daily_updates,
                      get_today, make_conn, pp_query, query_table, send_email)


def get_new_ie_transactions():
    os.environ['TODAY'] = get_today()
    engine = make_conn()
    df = pd.read_sql(f"select distinct(unique_id) from {TABLE}", con=engine)
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
        new_transactions.to_sql(TABLE, con=engine, if_exists="append")
        send_email(
            f"New Independent Expenditures for {os.getenv('TODAY', 'error')}!",
            new_transactions.to_json()
        )
        return f"Successfully updated with {len(bucket)} new transactions."
    else:
        return "No new transactions."


def load_content(committee_id: Union[str, None] = None) -> str:
    if committee_id is None:
        new_transactions = check_for_daily_updates()
        today = os.getenv("TODAY", "error")
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


def get_last_n_days(
        n: int = 1,
        fallback_n: int = 12
) -> pd.DataFrame:
    q = f"""
        SELECT *
        FROM {TABLE}
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {n} DAY)
        ORDER BY date DESC
        ;"""
    output = query_table(q)
    if output:
        return output
    else:
        q = f"""
            SELECT * FROM {TABLE}
            ORDER BY date DESC
            LIMIT {fallback_n}
            """
        output = query_table(q)
    return output
