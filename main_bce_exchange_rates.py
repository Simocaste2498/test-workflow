import csv
import io
from datetime import date, timedelta

import requests
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

"""
Scarica dall'ECB Data Portal tramite l'API SDMX il tasso di cambio USD/EUR
del giorno precedente e lo prepara per il caricamento in BigQuery.

Poiche la BCE pubblica le quotazioni solo nei giorni lavorativi, se il giorno
precedente e un weekend o una festivita viene utilizzata l'ultima quotazione
disponibile, mantenendo comunque la data di ieri nella riga caricata.

Per ogni giorno vengono create le colonne:
    - exchange_rate_usd_eur: valore originale restituito dalla BCE;
    - exchange_rate_eur_usd: reciproco del valore originale.

I dati vengono trasformati in un DataFrame pandas e aggiunti alla tabella
BigQuery con una merge.
"""


# Serie BCE giornaliera del cambio USD/EUR.
url = "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A"
# Data da caricare: il giorno precedente all'esecuzione dello script.
target_date = date.today() - timedelta(days=1)

# ---------------------------------------------------------------------------
# MODALITA INTERVALLO
# Per recuperare piu giorni, sostituire target_date con queste variabili e
# usare il blocco di elaborazione intervallo riportato piu avanti.
# start_date = date(2025, 1, 1)
# end_date = date(2026, 9, 24)
# target_date = end_date

# Configurazione BigQuery ripresa da gam_capped_opportunities.py.
BQ_JSON_KEY = "gcp_key.json"
PROJECT_ID = "bigdataitaetl-327308"
DATASET_ID = "operating_adv"
TABLE_NAME = "bce_exchange_rates"

params = {
    # La finestra precedente serve a trovare l'ultimo valore nei giorni festivi.
    "startPeriod": (target_date - timedelta(days=1)).isoformat(),
    "endPeriod": target_date.isoformat(),
}
headers = {"Accept": "text/csv"}

# In modalita intervallo, sostituire params con:
# params = {
#     "startPeriod": (start_date - timedelta(days=31)).isoformat(),
#     "endPeriod": end_date.isoformat(),
# }


response = requests.get(url, params=params, headers=headers, verify=False)
response.raise_for_status()

reader = csv.DictReader(io.StringIO(response.text))  #trasforma in un dizionario le righe del CSV restituito dalla BCE, con le intestazioni come chiavi
if not reader.fieldnames or "OBS_VALUE" not in reader.fieldnames:
    raise ValueError("La risposta BCE non contiene la colonna OBS_VALUE")

api_rows = list(reader)
rows_by_date = {
    date.fromisoformat(row["TIME_PERIOD"]): row for row in api_rows
}

# Usa la quotazione di ieri o, se ieri non e un giorno lavorativo, l'ultima
# quotazione disponibile. La data della riga resta comunque quella di ieri.
row = rows_by_date.get(target_date)
if row is None:
    previous_dates = [row_date for row_date in rows_by_date if row_date < target_date]
    if not previous_dates:
        raise ValueError(f"Nessun valore BCE disponibile per {target_date}")
    row = rows_by_date[max(previous_dates)].copy()
    row["TIME_PERIOD"] = target_date.isoformat()
else:
    row = row.copy()

# Calcola il cambio inverso EUR/USD per il giorno da caricare.
obs_value = row["OBS_VALUE"]
row["exchange_rate_usd_eur"] = obs_value
row["exchange_rate_eur_usd"] = "" if not obs_value or float(obs_value) == 0 else str(1 / float(obs_value))
del row["OBS_VALUE"]
output_rows = [row]

# MODALITA INTERVALLO: sostituire il blocco precedente con questo ciclo.
# previous_dates = [row_date for row_date in rows_by_date if row_date < start_date]
# last_row = rows_by_date[max(previous_dates)].copy() if previous_dates else None
# output_rows = []
# current_date = start_date
# while current_date <= end_date:
#     row = rows_by_date.get(current_date)
#     if row is None:
#         if last_row is None:
#             raise ValueError(f"Nessun valore BCE disponibile per {current_date}")
#         row = last_row.copy()
#         row["TIME_PERIOD"] = current_date.isoformat()
#     else:
#         last_row = row.copy()
#
#     obs_value = row["OBS_VALUE"]
#     row["exchange_rate_usd_eur"] = obs_value
#     row["exchange_rate_eur_usd"] = (
#         "" if not obs_value or float(obs_value) == 0
#         else str(1 / float(obs_value))
#     )
#     del row["OBS_VALUE"]
#     output_rows.append(row)
#     current_date += timedelta(days=1)

# DataFrame ridotto alle colonne da caricare nella tabella BigQuery.
df = pd.DataFrame(output_rows)
df["date"] = pd.to_datetime(df["TIME_PERIOD"]).dt.date
df["exchange_rate_usd_eur"] = pd.to_numeric(
    df["exchange_rate_usd_eur"], errors="coerce"
)
df["exchange_rate_eur_usd"] = pd.to_numeric(
    df["exchange_rate_eur_usd"], errors="coerce"
)
df = df[["date", "exchange_rate_usd_eur", "exchange_rate_eur_usd"]]


BQ_SCHEMA = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("exchange_rate_usd_eur", "FLOAT"),
    bigquery.SchemaField("exchange_rate_eur_usd", "FLOAT"),
]


def upload_df_to_bigquery(dataframe, table_name=TABLE_NAME):
    """Inserisce o aggiorna la quotazione del giorno in BigQuery."""
    client = bigquery.Client.from_service_account_json(
        BQ_JSON_KEY, project=PROJECT_ID
    )
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    dataframe = dataframe.drop_duplicates(subset=["date"], keep="last")

    try:
        client.get_table(table_id)
    except NotFound:
        # Prima esecuzione: crea la tabella con la prima quotazione.
        job_config = bigquery.LoadJobConfig(
            schema=BQ_SCHEMA,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date",
            ),
        )
        job = client.load_table_from_dataframe(
            dataframe, table_id, job_config=job_config
        )
        job.result()
        print(f"Uploaded to {table_id}")
        return

    #inserisco in una tabella temporanea i dati nuovi e poi faccio un merge con la tabella principale per aggiornare o inserire i valori.
    staging_table_id = f"{table_id}_staging"
    staging_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    staging_job = client.load_table_from_dataframe(
        dataframe, staging_table_id, job_config=staging_config
    )
    staging_job.result()

    merge_query = f"""
        MERGE `{table_id}` AS target
        USING `{staging_table_id}` AS source
        ON target.date = source.date
        WHEN MATCHED THEN
          UPDATE SET
            exchange_rate_usd_eur = source.exchange_rate_usd_eur,
            exchange_rate_eur_usd = source.exchange_rate_eur_usd
        WHEN NOT MATCHED THEN
          INSERT (date, exchange_rate_usd_eur, exchange_rate_eur_usd)
          VALUES (source.date, source.exchange_rate_usd_eur,
                  source.exchange_rate_eur_usd)
    """
    client.query(merge_query).result()
    client.delete_table(staging_table_id, not_found_ok=True)
    print(f"Update completato su {table_id}")


if __name__ == "__main__":
    upload_df_to_bigquery(df)
