import os
import json
from datetime import datetime, timezone

import requests
import pandas as pd
import gspread

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

#DOWNLOAD COHORTS DA PERMUTIVE (CON FILTRO SU TAGS CONTENENTE "ADV_AI") E UPLOAD SU GOOGLE SHEETS

# ==========================================================
# CONFIGURAZIONE
# ==========================================================

load_dotenv()

with open(
    "key_permutive.json",
    encoding="utf-8"
) as f:
    config = json.load(f)

API_URL = "https://api.permutive.app/cohorts-api/v2/cohorts"

API_KEY_DEFAULT = (
    config["permutive"]
    ["default_api_key"]
)

API_KEY_HBBTV = (
    config["permutive"]
    ["hbbtv_api_key"]
)

INSTANCES = [
    {
        "source": "default",
        "label": "Permutive Default",
        "api_key": API_KEY_DEFAULT
    },
    {
        "source": "hbbtv",
        "label": "Permutive HBBTV",
        "api_key": API_KEY_HBBTV
    }
]

SPREADSHEET_ID = (
    config["google"]
    ["spreadsheet_id"]
)
GOOGLE_CREDENTIALS_FILE = "gcp-sc-sa.json"
SHEET_NAME = "Cohorts"


# ==========================================================
# GOOGLE SHEETS
# ==========================================================

def get_google_client():

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    credentials = Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_FILE,
        scopes=scopes
    )

    return gspread.authorize(credentials)


# ==========================================================
# UTILS
# ==========================================================

def flatten_object(obj, prefix="", output=None):

    if output is None:
        output = {}

    if obj is None:
        if prefix:
            output[prefix] = ""
        return output

    if isinstance(obj, list):
        output[prefix] = json.dumps(
            obj,
            ensure_ascii=False
        )
        return output

    if isinstance(obj, dict):

        if not obj and prefix:
            output[prefix] = "{}"
            return output

        for key, value in obj.items():

            child = (
                f"{prefix}.{key}"
                if prefix
                else key
            )

            flatten_object(
                value,
                child,
                output
            )

        return output

    output[prefix] = obj

    return output


def without_query(data):

    if not data:
        return {}

    return {
        k: v
        for k, v in data.items()
        if k != "query"
    }


def sanitize(value):

    if value is None:
        return ""

    if isinstance(value, (dict, list)):
        value = json.dumps(
            value,
            ensure_ascii=False
        )

    text = str(value)

    if text.startswith(("=", "+", "@")):
        text = "'" + text

    if len(text) > 49000:
        text = text[:49000] + "… [truncated]"

    return text


# ==========================================================
# DOWNLOAD COHORTS
# ==========================================================

def get_cohort_list(payload):

    if isinstance(payload, list):
        return payload

    for key in [
        "cohorts",
        "data",
        "items",
        "results"
    ]:

        if (
            isinstance(payload, dict)
            and key in payload
            and isinstance(payload[key], list)
        ):
            return payload[key]

    raise Exception(
        "Formato risposta Permutive inatteso."
    )


def has_tag(data, target_tag):

    target_tag = str(target_tag).upper()

    if isinstance(data, dict):
        for key, value in data.items():
            if key == "tags" and isinstance(value, (list, tuple, set)):
                for item in value:
                    if str(item).upper() == target_tag:
                        return True

            if has_tag(value, target_tag):
                return True

        return False

    if isinstance(data, (list, tuple, set)):
        for item in data:
            if has_tag(item, target_tag):
                return True

        return False

    return str(data).upper() == target_tag


def download_all_cohorts():

    rows = []

    retrieved_at = (
        datetime.now(timezone.utc)
        .isoformat()
    )

    for instance in INSTANCES:

        if not instance["api_key"]:
            continue

        print(
            f"\n=== {instance['label']} ==="
        )

        headers = {
            "Accept": "application/json",
            "X-API-KEY":
                instance["api_key"]
        }

        response = requests.get(
            API_URL,
            headers=headers,
            timeout=60
        )

        response.raise_for_status()

        payload = response.json()

        cohorts = get_cohort_list(
            payload
        )

        print(
            f"Cohort trovate: {len(cohorts)}"
        )

        for cohort in cohorts:

            cohort_id = cohort.get("id")

            if not cohort_id:
                continue

            # Verifica preliminare se il tag ADV_AI è presente nella cohort
            if not has_tag(cohort, "ADV_AI"):
                continue

            print(
                f"Download detail {cohort_id}"
            )

            detail_response = requests.get(
                f"{API_URL}/{cohort_id}",
                headers=headers,
                timeout=60
            )

            detail_response.raise_for_status()

            detail_payload = (
                detail_response.json()
            )

            if (
                isinstance(
                    detail_payload,
                    dict
                )
                and "data"
                in detail_payload
            ):
                detail = (
                    detail_payload["data"]
                )
            else:
                detail = detail_payload

            merged = {}

            merged.update(
                cohort or {}
            )

            merged.update(
                detail or {}
            )

            flattened = flatten_object(
                without_query(
                    merged
                )
            )

            row = {
                "Retrieved At":
                    retrieved_at,

                "Source":
                    instance["source"],

                "Source Label":
                    instance["label"],

                "Detail Retrieved At":
                    retrieved_at,

                "Detail Status":
                    "FETCHED",

                "Query JSON":
                    json.dumps(
                        detail.get(
                            "query"
                        ),
                        ensure_ascii=False
                    )
                    if (
                        detail
                        and detail.get(
                            "query"
                        ) is not None
                    )
                    else "",

                **flattened,

                "Raw JSON":
                    json.dumps(
                        merged,
                        ensure_ascii=False
                    ),

                "Detail Raw JSON":
                    json.dumps(
                        detail,
                        ensure_ascii=False
                    )
            }

            rows.append(row)

    return rows


# ==========================================================
# DATAFRAME
# ==========================================================

def build_dataframe(rows):

    df = pd.DataFrame(rows)

    # Campi da includere nel foglio
    allowed_fields = {
        "Retrieved At",
        "Source",
        "Query JSON",
        "code",
        "name",
        "id",
        "state",
        "last_updated_at",
        "description",
        "live audience",
        "live_audience_size",
        "tags"
    }

    fixed_headers = [
        "Retrieved At",
        "Source",
        "Query JSON"
    ]

    priority_fields = [
        "code",
        "name",
        "id",
        "state",
        "last_updated_at"
    ]

    special_headers = []

    dynamic_headers = [

        c for c in df.columns

        if c not in fixed_headers
        and c not in special_headers
    ]

    priority_existing = [

        c for c in priority_fields

        if c in dynamic_headers
    ]

    remaining = sorted(
        [
            c for c in dynamic_headers
            if c not in priority_existing
        ]
    )

    ordered_columns = (
        fixed_headers
        + priority_existing
        + remaining
        + special_headers
    )

    # Filtra solo i campi consentiti
    ordered_columns = [
        c for c in ordered_columns
        if c in allowed_fields
    ]

    df = df[
        ordered_columns
    ]

    df = df.fillna("")
    df = df.map(sanitize)

    return df


# ==========================================================
# GOOGLE SHEETS
# ==========================================================

def upload_to_google_sheet(df):

    gc = get_google_client()

    spreadsheet = gc.open_by_key(
        SPREADSHEET_ID
    )

    try:

        worksheet = (
            spreadsheet.worksheet(
                SHEET_NAME
            )
        )

        worksheet.clear()   #cancella i dati esistenti

    except gspread.WorksheetNotFound:

        worksheet = (
            spreadsheet.add_worksheet(
                title=SHEET_NAME,
                rows=max(
                    len(df) + 100,
                    1000
                ),
                cols=max(
                    len(df.columns) + 20,
                    100
                )
            )
        )

    data = (
        [df.columns.tolist()]
        + df.values.tolist()
    )

    print(
        "Upload su Google Sheets..."
    )

    worksheet.update(data)

    print(
        "Upload completato."
    )


# ==========================================================
# MAIN
# ==========================================================

def main():

    print(
        "Download cohort Permutive..."
    )

    rows = (
        download_all_cohorts()
    )

    print(
        f"Cohort scaricate: {len(rows)}"
    )

    df = build_dataframe(
        rows
    )

    upload_to_google_sheet(
        df
    )

    print(
        "Sincronizzazione completata."
    )


if __name__ == "__main__":
    main()

