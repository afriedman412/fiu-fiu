import os
from time import sleep
from typing import Any, Dict, List, Union

import pandas as pd
import requests
from bs4 import BeautifulSoup
from flask import Response, render_template, request
from requests.exceptions import JSONDecodeError

from .helpers import (BASE_URL, DATA_COLUMNS, TABLE, encode_df_urls, get_today,
                      make_conn, pp_query, query_table, send_email,
                      sleep_after_execution, soup_to_dict_helper)


def get_daily_transactions(date: str = None) -> List[Dict[str, Any]]:
    """
    Gets independent expenditures for provided date. Default is today by EST.
    (Loaded with get_today())

    Input:
        date (str): date in DT_FORMAT format or None

    Output:
        output (list): list of transactions
    """
    #  add test for failed pp query when auth gets figured out
    url = os.path.join(BASE_URL, "independent_expenditures/{}/{}/{}.json")

    #  make class for dates to ensure they follow DT_FORMAT
    if not date:
        date = get_today()
    url = url.format(*date.split("-"))
    bucket = []
    offset = 0
    while True:  # do this better eventually.
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
    today_transactions = get_daily_transactions()
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
        transactions = get_daily_transactions()
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
        if not committee_ie:
            committee_name = get_committee_name(committee_id)
            df = "No Results Found"
        else:
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
            df = df.to_html(escape=False)

        filename = f"{committee_id}_ie.csv"
        if request.method != "POST":
            return render_template(
                'committee_ie.html',
                committee_name=committee_name,
                committee_id=committee_id,
                df_html=df
            )

    return Response(
        df.to_csv(index=False),
        mimetype="text/csv",
        headers={
            "Content-disposition":
            "attachment; filename={}".format(filename)
        })


def get_committee_ie(committee_id: str) -> List[Any]:
    """
    Get independent expenditures for provided committee_id.
    """
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
                sleep(3)
            else:
                break
        else:
            raise Exception(f"Bad Status Code: {r.status_code}, {r.content}")
    return results


def get_fallback_data(
        n: int = 12,
        last_days: int = 0
) -> pd.DataFrame:
    """
    If no there is no data for the requested date, use this instead.

    Returns last "n" expenses or expenses from last "last_days" days.

    Input:
        n (int): number of most recent expenses to return
        last_days (int): number of days of recent expenses to return (trumps n)

    Output:
        output (pd.DataFrame): query result
    """
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


def get_daily_filings(date: str) -> dict[str, Union[pd.DataFrame, str]]:
    """
    Gets 24/48 hour filings for "date".
    These are individual expenses that may have not shown up in the rest of the data yet.
    They have their own endpoint.

    output format is to conform with downstream formatting

    Input:
        date (str): filing date, formatted "YYYY-MM-DD" (or "%Y-%m-%d", aka DT_FORMAT)

    Output:
        output (dict): {'24- and 48- Hour Filings': str or dataframe}
        ... dataframe if successful
        ... text if no forms found or if query returns an error.
    """
    url = os.path.join(
        BASE_URL,
        "filings/{}/{}/{}.json".format(
            *date.split("-"))
    )
    api_response = pp_query(url, error_out=False)
    if api_response.status_code == 200:
        try:
            api_data = [
                f for f in api_response.json()['results']
                if f['form_type'] in ['F6', 'F24']
            ]
            api_data = pd.DataFrame(api_data)
        except JSONDecodeError:
            api_data = f"No data found for {date}"
    else:
        api_data = f"Bad status code: {api_response.status_code}, {api_response.json().get(
            'message',
            'PROBLEM RETREIVING ERROR MESSAGE'
        )}"
    return {'24- and 48- Hour Filings': api_data}


def format_dates_output(
        data: Dict[str, pd.DataFrame],
        COLUMNS=DATA_COLUMNS
) -> Dict[str, str]:
    """
    Processes output of `get_data_by_date` for web presentation.
    (escapes urls and converts dataframe to html)

    Making this its own function makes testing easier!
    """
    if COLUMNS:
        data = {k: v[COLUMNS] for k, v in data.items()}
    return {k: encode_df_urls(v).to_html(escape=False) for k, v in data.items()}


def download_dates_output(data: Dict[str, pd.DataFrame]):
    if len(data) > 1:
        df = pd.DataFrame()
        for k in data:
            data[k]['query'] = k
            df = df.append(data[k])

    else:
        df = data.values()[0]

    Response(
        df.to_csv(index=False),
        mimetype="text/csv",
        headers={
            "Content-disposition":
            "attachment; filename={}".format("date_data.csv")
        })


@sleep_after_execution(3)
def parse_24_48(url: str) -> Dict[str, str]:
    """
    24- and 48- Hour filing expenses are often not yet in the data.
    They need to be scraped manually from the web (as far as I can tell!).

    Retrieves "url" and uses (key, regex) pairs to pull data out of the web response.

    Different key/regex pairs for 24 and 48 hour forms.

    If request is bad, returns a dict reflecting that with status code and error message.

    Automatically sleeps 3 after each run!

    Input:
        url (str): url for expense

    Output:
        output (dict): dict of expense data or error info
    """
    # for line length restrictions
    h = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    headers = {'User-Agent': h}
    r = requests.get(
        url,
        timeout=30,
        headers=headers
    )
    print('request ran...')
    if r.status_code != 200:
        return {'error': f'Status code {r.status_code}, {str(r.content)}'}
    else:
        soup = BeautifulSoup(r.content)
        if "48 HOUR NOTICE" in soup.text:
            output = {
                i: soup_to_dict_helper(j, soup.text)
                for i, j in [
                    ("filing_id", r"\nFILING"),
                    ("committee_name", r"\n1\. "),
                    ("committee_id", r"4\. FEC Committee ID \#\: "),
                    ("candidate_id", r"Candidate ID \#\: "),
                    ("candidate_name", r"Candidate Name\: "),
                    ("office_sought", r"3\. Office Sought\: "),
                ]
            }
            output['transactions_link'] = os.path.join(url, "f65")
            output['form_type'] = "48"
        else:
            output = {
                i: soup_to_dict_helper(j, soup.text)
                for i, j in [
                    ("filing_id", r"24 HOUR NOTICE FILING "),
                    ("committee_id", r"\nFEC Committee ID \#\: "),
                ]
            }
            output['committee_name'] = soup.title.text.strip().replace("Form 24 for ", "")
            output['expenditures_link'] = os.path.join(url, "se")
            output['form_type'] = "24"
    return output


def get_committee_name(committee_id: str) -> str:
    """
    Gets the name of a committee from its id.

    Need to do this to get the committee name if there aren't any results!

    Input:
        committee_id (str): FEC committee id

    Output:
        output (str): name of committee or "" if none found
    """
    url = os.path.join(BASE_URL, f"committees/{committee_id}.json")
    r = pp_query(url)
    try:
        return r.json()['results'][0]['name']
    except KeyError:
        return ""
