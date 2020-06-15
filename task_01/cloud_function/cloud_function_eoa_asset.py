from google.cloud import bigquery
import pandas
import json

# bigquery client 인스턴스 생성
client = bigquery.Client()

# 특정 eoa_address에 대하여 bigquery에 쿼리문을 실행하고 리턴 값 받음
def get_asset_info(eoa_address):
    # 쿼리문
    query = """
        SELECT ARRAY(
        SELECT AS STRUCT token_id as token_id, token_address as token_address
            ) as asset
        FROM `bigquery-279701.take_home.eoa_tkid_tkaddr`
        WHERE eoa_address LIKE "%s"
        """ % (eoa_address)
    
    # bigquery에 해당 쿼리문을 실행하고 리턴 값 받아옴
    query_job = client.query(query)  # Make an API request.
    return query_job.result().to_dataframe().to_json()

def main(request):
    request_json = request.get_json(silent=True)
    return get_asset_info(request_json['eoa_address'])
