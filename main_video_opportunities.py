import os
import time
import re
from pprint import pformat
from warnings import filters
import pandas as pd
import requests
from google.cloud import bigquery
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account


BASE_URL = "https://admanager.googleapis.com/v1"  #endpoint REST API (versione v1)
SCOPE = "https://www.googleapis.com/auth/admanager" #fornito per la generazione del token, definisce i permessi del token per 
                                                    #l'accesso alle risorse di GAM

BQ_JSON_KEY = "gcp_key.json"
GAM_JSON_KEY = "gam_key.json"
NETWORK_CODE = "35821442"
APPLICATION_NAME = "GAM Report Downloader REST"
PAGE_SIZE = 10000  #numero di righe per ogni chiamata fecthRows. 
                   #Se il report ha più righe, GAM restituisce anche un nextPageToken; 
                   # lo script lo usa nel while per scaricare la pagina successiva, finché non finiscono le righe.
OUTPUT_CSV = "gam_capped_opportunities.csv"

PROJECT_ID = "bigdataitaetl-327308"
DATASET_ID = "operating_adv"
TABLE_NAME = "gam_video_capped_opportunities"


# Optional. Set GAM_REPORT_RESOURCE_NAME to reuse an existing report resource:
# networks/{network_code}/reports/{report_id}
REPORT_RESOURCE_NAME = os.getenv("GAM_REPORT_RESOURCE_NAME")

# Optional. Set DEVICE_CATEGORY_ID to reproduce the SOAP Statement filter.
DEVICE_CATEGORY_ID = os.getenv("DEVICE_CATEGORY_ID")

#REST dimensions names
REPORT_DIMENSIONS = [
    "DATE",
    "AD_UNIT_ID",
    "AD_UNIT_NAME_ALL_LEVEL",
    "AD_UNIT_NAME_TOP_LEVEL",
    "AD_UNIT_NAME",
]

# REST metric names 
REPORT_METRICS = [
    "VIDEO_TRUE_OPPORTUNITIES_TOTAL_CAPPED_OPPORTUNITIES_ADBREAK",
]


def get_session(json_key):
    credentials = service_account.Credentials.from_service_account_file(
        json_key,
        scopes=[SCOPE],
    )
    return AuthorizedSession(credentials)


def raise_for_status(response):
    try:
        response.raise_for_status()
    except requests.HTTPError as error:
        try:
            body = response.json()
        except ValueError:
            body = response.text
        message = (
            f"{error}\n\n"
            f"Response body from Google Ad Manager:\n{pformat(body)}"
        )
        raise requests.HTTPError(message, response=response) from error


def make_report_definition():

    filters = []

    if DEVICE_CATEGORY_ID:
        filters.append(
                {
                    "fieldFilter": {
                        "field": {"dimension": "REQUEST_TYPE_NAME"},
                        "operation": "IN",
                        "values": [{"stringValue": "Video Tag"}],
                    }
                }
            )
  
        
    definition = {
        "reportType": "HISTORICAL",
        "dateRange": {"relative": "YESTERDAY"},
        #"dateRange": {"fixed": {"startDate": {"year": 2026, "month": 6, "day": 22}, "endDate": {"year": 2026, "month": 6, "day": 23}}},
        "dimensions": REPORT_DIMENSIONS,
        "metrics": REPORT_METRICS,
    }

    if filters:
        definition["filters"] = filters

    return definition


def create_report(session, network_code):
    report = {
        "displayName": f"report_template_rest_{int(time.time())}",
        "visibility": "HIDDEN",
        "reportDefinition": make_report_definition(),
    }

    response = session.post(
        f"{BASE_URL}/networks/{network_code}/reports",
        json=report,
        headers={"x-goog-request-params": f"parent=networks/{network_code}"},
    )
    raise_for_status(response)
    return response.json()["name"]


def run_report(session, report_name):
    response = session.post(f"{BASE_URL}/{report_name}:run")  #invia richiesta HTTP POST a Gam per lanciare il report
    raise_for_status(response)
    operation = response.json()  #risposta di GAM dopo aver lanciato il report, i.e. ticket di lavorazione del report

    sleep_seconds = 5  #minimo tempo di attesa tra un controllo e il successivo per vedere se GAM ha generato il report
    #Steps: report lanciato con :run, risposta di gam con operation, ma il report potrebbe non essere ancora pronto.
    #Lo script aspetta 5 secondi, controlla se è finito, poi aspetta il doppio dei secondi ecc fino a un max di 60 (exponential backoff)
    while not operation.get("done"):
        time.sleep(sleep_seconds)
        response = session.get(f"{BASE_URL}/{operation['name']}")
        raise_for_status(response)
        operation = response.json()
        sleep_seconds = min(sleep_seconds * 2, 60)

    if "error" in operation:
        raise RuntimeError(f"Report run failed: {operation['error']}")

    return operation["response"]["reportResult"]


#Converto la risposta JSON dell'API in un formato Python da usare in un dataframe
def report_value_to_python(value):
    if not value:
        return None

    for key in ("stringValue", "intValue", "doubleValue", "boolValue", "bytesValue"):
        if key in value:
            return value[key]

    for key in ("stringListValue", "intListValue", "doubleListValue"):
        if key in value:
            return value[key].get("values", [])

    return value


#Scarica le righe del report e le converte in un dataframe pandas
def fetch_report_rows(session, report_result, dimensions, metrics):
    rows = []
    page_token = None
    seen_dates = set()

    while True:
        params = {"pageSize": PAGE_SIZE}
        if page_token:
            params["pageToken"] = page_token

        response = session.get(f"{BASE_URL}/{report_result}:fetchRows", params=params) #la chiamata GET serve a recuperare le righe del report
        raise_for_status(response)
        payload = response.json()

        for row in payload.get("rows", []):
            flat_row = {}

            for dimension, value in zip(dimensions, row.get("dimensionValues", [])):
                flat_row[f"Dimension.{dimension}"] = report_value_to_python(value)
                # Stampa la data quando la incontra per la prima volta
                if dimension == "DATE":
                    date_val = report_value_to_python(value)
                    if date_val and date_val not in seen_dates:
                        seen_dates.add(date_val)
                        print(f"Processing date: {date_val}")

            metric_groups = row.get("metricValueGroups", [])
            for group_index, group in enumerate(metric_groups):
                suffix = "" if len(metric_groups) == 1 else f".range_{group_index + 1}"
                values = group.get("primaryValues", [])
                for metric, value in zip(metrics, values):
                    flat_row[f"Column.{metric}{suffix}"] = report_value_to_python(value)

            rows.append(flat_row)

        page_token = payload.get("nextPageToken")
        if not page_token:
            break

    return pd.DataFrame(rows)


def clean_bigquery_column_name(column_name):
    column_name = column_name.replace("Dimension.", "")
    column_name = column_name.replace("Column.", "")
    column_name = re.sub(r"[^A-Za-z0-9_]", "_", column_name)
    column_name = column_name.strip("_").lower()

    if not column_name:
        return "field"
    if column_name[0].isdigit():
        return f"field_{column_name}"
    return column_name


def prepare_df_for_bigquery(df):
    df = df.copy()
    df.columns = [clean_bigquery_column_name(column) for column in df.columns]

    df["ad_unit_name_all_level"] = df["ad_unit_name_all_level"].apply(
    lambda x: " >> ".join(x) if isinstance(x, list) else x
    )

    df = df.rename(columns={
    "ad_unit_name_all_level": "ad_unit_name",
    "ad_unit_name_top_level": "top_adunit",
    "ad_unit_name": "position_adunit",
    "video_true_opportunities_total_capped_opportunities_adbreak": "capped_opportunities"
   })

    #converto nel tipo di dato desiderato per il caricamento su BigQuery
    df["date"] = pd.to_datetime(df["date"]).dt.date

    string_cols = [
        "ad_unit_id",
        "ad_unit_name",
        "top_adunit",
        "position_adunit",
    ]

    for col in string_cols:
        df[col] = df[col].astype("string")

    metric_cols = [
        "capped_opportunities",
    ]

    for col in metric_cols:
        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        ).astype("Int64")
    return df

# Function to upload DataFrame to BigQuery
def upload_df_to_bigquery(df, table_name):
    client = bigquery.Client.from_service_account_json(BQ_JSON_KEY, project=PROJECT_ID)
    #job_config = bigquery.job.LoadJobConfig(schema=BQ_SCHEMA)
    #job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,  #se la tabella non esiste, la crea
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning( type_=bigquery.TimePartitioningType.DAY, field="date", ),
    )
    table_id = "{}.{}.{}".format(PROJECT_ID, DATASET_ID, table_name)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print("Uploaded to {}".format(table_id))


def get_report(session, network_code):
    report_name = REPORT_RESOURCE_NAME or create_report(session, network_code)
    report_result = run_report(session, report_name)
    return fetch_report_rows(session, report_result, REPORT_DIMENSIONS, REPORT_METRICS)


#definisco lo schema della tabella BigQuery in cui caricare il report
BQ_SCHEMA = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("ad_unit_id", "STRING"),
    bigquery.SchemaField("ad_unit_name", "STRING"),
    bigquery.SchemaField("top_adunit", "STRING"),
    bigquery.SchemaField("position_adunit", "STRING"),
    bigquery.SchemaField("capped_opportunities", "INTEGER"),
]

if __name__ == "__main__":
    test_start_time = time.time()

    session = get_session(GAM_JSON_KEY)
    report = get_report(session, NETWORK_CODE)
    report.to_csv(OUTPUT_CSV, index=False)
    upload_df_to_bigquery(prepare_df_for_bigquery(report), TABLE_NAME)

    test_end_time = time.time()
    total_time = test_end_time - test_start_time

    print(f"Rows downloaded: {len(report)}")
    print(f"Saved report to: {OUTPUT_CSV}")
    print(f"Total Test Time: {total_time:.2f} seconds\n")
