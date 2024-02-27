import os
from time import sleep
from typing import Union, List

import pandas as pd
from flask import Response, render_template, request

from .helpers import (DISPLAY_COLUMNS, EMAIL_COLUMNS, TABLE, BASE_URL,
                      get_today, make_conn, pp_query,
                      query_table, send_email)


def get_today_transactions():
    #  add test for failed pp query when auth gets figured out
    #  this should be os.environ['CYCLE']
    url = os.path.join(BASE_URL, "independent_expenditures/{}/{}/{}.json")
    url = url.format(*get_today().split("-"))
    print(url)
    bucket = []
    offset = 0
    while True:
        r = pp_query(url, offset=offset)
        if r.status_code == 200:
            transactions = r.json()['results']
            if transactions:
                bucket += transactions
                offset += 20
                sleep(3)
            else:
                break
        else:
            raise Exception(", ".join(
                [
                    str(r.status_code),
                    r.json().get(
                        "message", "error retrieving error message"
                    )]))
    return bucket


def get_exisiting_today_ids() -> List[str]:
    existing_today_ids = [
        i[0]
        for i in
        query_table(
            "select unique_id from fiu_pp where date_received='{}'".format(
                os.getenv("TODAY")
            )
        )]
    return existing_today_ids


def get_new_ie_transactions():
    today_transactions = get_today_transactions()
    existing_today_ids = get_exisiting_today_ids()
    new_today_transactions = [
        t for t in today_transactions
        if t['unique_id'] not in existing_today_ids
    ]
    if new_today_transactions:
        new_today_transactions_df = pd.DataFrame(new_today_transactions)
        engine = make_conn()
        new_today_transactions_df.to_sql(TABLE, con=engine, if_exists="append")
        send_email(
            f"New Independent Expenditures for {os.getenv('TODAY', 'error')}!",
            new_today_transactions_df[EMAIL_COLUMNS].to_html()
        )
    return new_today_transactions


def load_content(committee_id: Union[str, None] = None) -> str:
    if committee_id is None:
        transactions = get_today_transactions()
        new_transactions = True
        if not transactions:
            new_transactions = False
            transactions = get_fallback_data()
        today = os.getenv("TODAY", "error")
        df = pd.DataFrame(transactions)
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
    url = os.path.join(BASE_URL, "committees/{}/independent_expenditures.json")
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


def get_fallback_data(
        n: int = 12,
        last_days: int = 0
) -> pd.DataFrame:
    if last_days:
        q = f"""
            SELECT *
            FROM {TABLE}
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {last_days} DAY)
            ORDER BY date DESC
            ;"""
        output = query_table(q)
        if output:
            return output
    else:
        q = f"""
            SELECT * FROM {TABLE}
            ORDER BY date DESC
            LIMIT {n}
            """
        output = query_table(q)
    return output
