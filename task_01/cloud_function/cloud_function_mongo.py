import requests 
import pymongo
import json

OPENSEA_URL = "https://api.opensea.io/api/v1/assets"
MONGO_CLIENT_URL = "mongodb+srv://ecosy:2112@cluster0-mqgxy.gcp.mongodb.net/"

def get_data_from_opensea(request):
    request_json = request.get_json(silent=True)
    print(request_json)

    params = {"order_direction":"desc", "limit":"50"}
    params['token_ids'] = request_json['value']
    params['asset_contract_address'] = request_json['token_address']

    # opensea.io api - asset을 조회함
    response = requests.request("GET", OPENSEA_URL, params=params)

    # api 리턴값 - 자산 정보
    return json.loads(response.text)

def insert_mongoDB(asset):
    # MongoDB에 삽입할 dictionary 자산 데이터 
    data = {}
    data['eoa_address'] = asset['assets'][0]['owner']['address']
    data['token_id'] = asset['assets'][0]['token_id']
    data['token_address'] = asset['assets'][0]['asset_contract']['address']
    data['item_name'] = asset['assets'][0]['name']

    print(data)
    
    # MongoDB client 인스턴스 생성
    client = pymongo.MongoClient(MONGO_CLIENT_URL)
    db = client['take-home']
    collection = db.eoa_item

    # MongoDB에 자산정보 삽입
    collection.insert_one(data)

    return "success"

def main(request):
    asset_info = get_data_from_opensea(request)
    insert_mongoDB(asset_info)