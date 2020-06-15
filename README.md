# Task Summary 
## 1. 전체 진행상황
* task 1 완료
* task 2 미완료

## 2. 프로젝트 진행 과정
#### (2.1) 인터뷰 가이드를 읽고 모르는 것, 조사할 내용 정리하여 공부함
#### (2.2) input-output 데이터의 관계를 보고 어떤 데이터가 필요한지 알아봄
#### (2.3) 필요한 데이터 수집 source, 수집 방법, 파이프라인 설계함
#### (2.4) 코드로 구현함

## 3. Task 1
#### (3.1) 프로젝트 결과
* cloud function 이용한 api 구현
* 유저의 eoa_address 입력에 따라 소유 자산의 token_address, token_id 리턴

#### (3.2) api 실행 방법
* python 코드
```
import requests
import json

url = "https://us-central1-bigquery-279701.cloudfunctions.net/eoa_asset"
params = {'eoa_address' : "0x138a35ee20e40f019e7e7c00386ab2ef42d66d1e"}
headers = {'Content-type': 'application/json'}

response = requests.post(url, headers=headers, data=json.dumps(params))
print(response.text)
```

#### (3.3) 파이프라인
#### Bigquery -> cloud function / tasks queue -> mongoDB -> Bigquery -> api (cloud function)

1. bigquery-public-data:crypto_ethereum 에서 3가지 서비스에 해당되는 **token_address, value** 추출함
* 관련 코드 : [extract_token_addr_bigquery.sql](https://github.com/ecosy/take-home/blob/master/task_01/bigquery_sql/extract_token_addr_bigquery.sql)

2. opensea.io api를 사용하여 1번의 value에 대해 현재 소유한 유저의 **eoa_address** 가져옴    

3. 2번의 결과물 [eoa_address, token_address, token_id]를 mongoDB에 저장함   
* 2번, 3번의 과정은 Cloud Function, Google Tasks Queue를 사용함

* cloud function 관련 코드 : [cloud_function_mongo.py](https://github.com/ecosy/take-home/blob/master/task_01/cloud_function/cloud_function_mongo.py)

* cloud function call 관련 코드 : 아래의 6개 파일
  * 총 108856개의 value에 대해서 index별로 아래의 6개 파일로 나누어서 처리했고,
각 파일은 총 3개의 google tasks queue로 데이터를 나누어 보냄    
  - index[0 - 20000] -> [pipe_01_bigquery_to_mongoDB.ipynb](https://github.com/ecosy/take-home/blob/master/task_01/pipeline_code/pipe_01_bigquery_to_mongoDB.ipynb)
  - index[20000 - 40000] -> [pipe_01_bigquery_to_mongoDB_2.ipynb](https://github.com/ecosy/take-  home/blob/master/task_01/pipeline_code/pipe_01_bigquery_to_mongoDB_2.ipynb) 
  - index[40000 - 60000] -> [pipe_01_bigquery_to_mongoDB_3.ipynb](https://github.com/ecosy/take-home/blob/master/task_01/pipeline_code/pipe_01_bigquery_to_mongoDB_3.ipynb)
  - index[60000 - 80000] -> [pipe_01_bigquery_to_mongoDB_4.ipynb](https://github.com/ecosy/take-home/blob/master/task_01/pipeline_code/pipe_01_bigquery_to_mongoDB_4.ipynb)
  - index[80000 - 100000] -> [pipe_01_bigquery_to_mongoDB_5.ipynb](https://github.com/ecosy/take-home/blob/master/task_01/pipeline_code/pipe_01_bigquery_to_mongoDB_5.ipynb)
  - index[100000 - 108856] -> [pipe_01_bigquery_to_mongoDB_6.ipynb](https://github.com/ecosy/take-home/blob/master/task_01/pipeline_code/pipe_01_bigquery_to_mongoDB_6.ipynb)

4. MongoDB에 저장된 데이터를 pandas DataFrame으로 변환, BigQuery에 업로딩함
* 관련 코드 : [pipe_02_mongoDB_to_bigquery.ipynb](https://github.com/ecosy/take-home/blob/master/task_01/pipeline_code/pipe_02_mongoDB_to_bigquery.ipynb)

5. BigQuery에 업로딩된 데이터에 대하여 api 코드 구현
* 관련 코드 : [cloud_function_eoa_asset.py](https://github.com/ecosy/take-home/blob/master/task_01/cloud_function/cloud_function_eoa_asset.py)
