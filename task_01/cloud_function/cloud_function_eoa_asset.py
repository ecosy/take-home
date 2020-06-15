from google.cloud import bigquery
import pandas
import json

# Construct a BigQuery client object.
client = bigquery.Client()


def get_asset_info(eoa_address):
    query = """
        SELECT ARRAY(
        SELECT AS STRUCT token_id as token_id, token_address as token_address
            ) as asset
        FROM `bigquery-279701.take_home.eoa_tkid_tkaddr`
        WHERE eoa_address LIKE "%s"
        """ % (eoa_address)

    query_job = client.query(query)  # Make an API request.
    return query_job.result().to_dataframe().to_json()

def main(request):
    request_json = request.get_json(silent=True)
    return get_asset_info(request_json['eoa_address'])