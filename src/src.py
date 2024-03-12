import os
from time import sleep
from typing import Any, Dict, List, Union

import pandas as pd
from flask import Response, render_template, request
import requests
from bs4 import BeautifulSoup
import re

from .helpers import (BASE_URL, DATA_COLUMNS, TABLE, get_today, make_conn,
                      pp_query, query_table, send_email, soup_to_dict_helper, encode_df_urls)


def get_today_transactions():
    #  add test for failed pp query when auth gets figured out
    url = os.path.join(BASE_URL, "independent_expenditures/{}/{}/{}.json")
    url = url.format(*get_today().split("-"))
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


def get_exisiting_ids() -> List[str]:
    existing_today_ids = [
        i[0]
        for i in
        query_table(
            "select distinct unique_id from fiu_pp"
        )]
    return existing_today_ids


def get_new_ie_transactions(send_email_trigger: bool = True):
    today_transactions = get_today_transactions()
    existing_ids = get_exisiting_ids()
    new_today_transactions = [
        t for t in today_transactions
        if t['unique_id'] not in existing_ids
    ]
    if new_today_transactions:
        new_today_transactions_df = pd.DataFrame(new_today_transactions)
        engine = make_conn()
        new_today_transactions_df.to_sql(TABLE, con=engine, if_exists="append")
        if send_email_trigger:
            send_email(
                f"New Independent Expenditures for {os.getenv('TODAY', 'error')}!",
                new_today_transactions_df[DATA_COLUMNS].to_html()
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
        df = df[DATA_COLUMNS].sort_values(
            ['date', 'date_received'],
            ascending=False
        )
        df = encode_df_urls(df)
        
        filename = f"ie_{today}.csv"
        if request.method != "POST":
            df_html = df.to_html(escape=False) if len(df) else None
            return render_template(
                'index.html',
                today=today,
                new_transactions=new_transactions,
                df_html=df_html
            )

    else:
        committee_ie = get_committee_ie(committee_id)
        committee_name = committee_ie[0].get('fec_committee_name')
        df = pd.DataFrame(committee_ie)
        try:
            df = df[DATA_COLUMNS].sort_values(
                ['date', 'date_received'],
                ascending=False
            )
            df = encode_df_urls(df)
        except KeyError:
            pass
        filename = f"{committee_id}_ie.csv"
        if request.method != "POST":
            df_html = df.to_html(escape=False)
            return render_template(
                'committee_ie.html',
                committee_name=committee_name,
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


def get_committee_ie(committee_id: str) -> List[Any]:
    url = os.path.join(BASE_URL, "committees/{}/independent_expenditures.json")
    url = url.format(
        committee_id
    )
    results = []
    offset = 0
    while True:
        r = pp_query(url, offset)
        if r.status_code == 200:
            if r.json().get('results'):
                print(len(r.json().get('results')))
                results += r.json().get('results')
                offset += 20
            else:
                break
        else:
            raise Exception(f"Bad Status Code: {r.status_code}, {r.content}")
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


def get_data_by_date(date: str, *args) -> dict[str, pd.DataFrame]:
    """
    Gets expenditure data by date, from the table and from the propublica API.

    Because it's unclear what the canonical date is, we can check all at once.

    Input:
        date (str): date in format "YYYY-MM-DD" from datepicker
        args (str): 'api', 'date', 'date_received', 'dissemination_date' from form

    Output:
        output (dict): dictionary of data sources and expenditure data in df
    """
    output = {}
    for k in args:
        if k in ['date', 'date_received', 'dissemination_date']:
            table_data = query_table(
                f"select * from {TABLE} where {k}='{date}'"
            )
            output[k] = pd.DataFrame(table_data)
        if k == 'api':
            url = os.path.join(
                BASE_URL,
                "independent_expenditures/{}/{}/{}.json".format(
                    *date.split("-"))
            )
            api_data = pp_query(url)
            output[k] = pd.DataFrame(api_data.json()['results'])
    return output


def get_daily_filings(date: str) -> dict[str, pd.DataFrame]:
    url = os.path.join(
            BASE_URL,
            "filings/{}/{}/{}.json".format(
                *date.split("-"))
        )
    api_data = pp_query(url)
    if api_data.json()['results']:
        api_data = [
            f for f in api_data.json()['results'] 
            if f['form_type'] in ['F6', 'F24']
        ]
    else:
        api_data = []
    return {'24- and 48- Hour Filings': pd.DataFrame(api_data)}


def format_dates_output(
        data: Dict[str, pd.DataFrame],
        COLUMNS=DATA_COLUMNS
        ) -> Dict[str, str]:
    """
    Processes output of `get_data_by_date` for web presentation.

    Making this its own app makes testing easier!
    """
    if COLUMNS:
        return {k: encode_df_urls(v[COLUMNS]).to_html(escape=False) for k, v in data.items()}
    else:
        return {k: encode_df_urls(v).to_html(escape=False) for k, v in data.items()}


def download_dates_output(data: Dict[str, pd.DataFrame]):
    if len(data) > 1:
        df = pd.DataFrame()
        for k in data:
            data[k]['query'] = k
            df = df.append(data[k])

    else:
        df = data[k]

    Response(
        df.to_csv(index=False),
        mimetype="text/csv",
        headers={
            "Content-disposition":
            "attachment; filename={}".format("date_data.csv")
        })

def parse_24_48(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        return
    else:
        soup = BeautifulSoup(r.content)
        if "48 HOUR NOTICE" in soup.text:
            output = {
                i: soup_to_dict_helper(j, soup.text)
                for i, j in [
                    ("filing_id", "\nFILING"),
                    ("committee_name", "\n1\. "),
                    ("committee_id", "4\. FEC Committee ID \#\: "),
                    ("candidate_id", "Candidate ID \#\: "),
                    ("candidate_name", "Candidate Name\: "),
                    ("office_sought", "3\. Office Sought\: "),
                ]
            }
            output['transactions_link'] = os.path.join(url, "f65")
            output['form_type'] = "48"
        else:
            output = {
                i: soup_to_dict_helper(j, soup.text)
                for i, j in [
                    ("filing_id", "24 HOUR NOTICE FILING "),
                    ("committee_id", "\nFEC Committee ID \#\: "),
                ]
            }
            output['committee_name'] = soup.title.text.strip().replace("Form 24 for ", "")
            output['expenditures_link'] = os.path.join(url, "se")
            output['form_type'] = "24"
    return output
