from googleads import ad_manager, oauth2
from google.cloud import bigquery
import datetime
import pandas as pd
import json
from zeep import helpers
import os

# Function to flatten and format a dictionary
def flatten_and_format_dict(data):
    result = {}

    for key, value in data.items():
        if key in ('thirdPartyMeasurementSettings', 'effectiveAppliedLabels', 'creativePlaceholders'):
            # Skip certain keys
            pass
        elif isinstance(value, dict) and key in ('startDateTime', 'endDateTime', 'lastModifiedDateTime', 'lastReservationDateTime'):
            # Extract and format datetime
            date = value['date']
            time = value.get('time', {'hour': 0, 'minute': 0, 'second': 0})
            datetime_str = datetime.datetime(date['year'], date['month'], date['day'],
                                             time['hour'], time['minute'], time['second']).strftime('%Y-%m-%d %H:%M:%S')
            result[key] = datetime_str
        elif isinstance(value, dict):
            # Flatten nested dictionary
            for inner_key, inner_value in value.items():
                result[f"{key}_{inner_key}"] = inner_value
        else:
            # Copy non-dict values directly
            result[key] = value

    return result

# Function to retrieve all objects from a Google Ad Manager service
def get_all_service_object(service_name, json_key, version='v202502'):
    oauth2_client = oauth2.GoogleServiceAccountClient(json_key, oauth2.GetAPIScope('ad_manager'))
    client = ad_manager.AdManagerClient(oauth2_client, APPLICATION_NAME, network_code=35821442)

    service = client.GetService(service_name, version=version)
    statement = (ad_manager.StatementBuilder(version=version))
    results = []
    while True:
        response = None
        if service_name == 'ProposalLineItemService':
            response = service.getProposalLineItemsByStatement(statement.ToStatement())
        elif service_name == 'ProposalService':
            response = service.getProposalsByStatement(statement.ToStatement())
        elif service_name == 'LineItemService':
            response = service.getLineItemsByStatement(statement.ToStatement())
        elif service_name == 'PlacementService':
            response = service.getPlacementsByStatement(statement.ToStatement())

        if not response or len(response['results']) == 0:
            break
        results.extend(response['results'])
        print(len(results), response['totalResultSetSize'])
        statement.offset += statement.limit
    return results

# Function to upload DataFrame to BigQuery
def upload_df_to_bigquery(df, table_name):
    client = bigquery.Client()
    job_config = bigquery.job.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job = client.load_table_from_dataframe(df, "{}.{}.{}".format(PROJECT_ID, DATASET_ID, table_name), job_config=job_config)
    print("Uploaded to {}".format(table_name))

# Main function
if __name__ == "__main__":
    # BigQuery configuration
    PROJECT_ID = 'bigdataitaetl-327308'
    DATASET_ID = 'operating_adv'
    JSON_KEY = "key.json"
    APPLICATION_NAME = "GAM Downloader"

    services = ['PlacementService']

    for service_name in services:
        table_name = service_name.lower() + '_test_sc'
        print("Getting results for {}".format(service_name))
        results = get_all_service_object(service_name, JSON_KEY, version='v202502')
        print("Serializing results for {}".format(service_name))
        serialized_data = json.loads(json.dumps([helpers.serialize_object(item) for item in results]))
        df = pd.DataFrame([flatten_and_format_dict(x) for x in serialized_data])
        print("Uploading {} to Bigquery".format(service_name))
        upload_df_to_bigquery(df, table_name)




