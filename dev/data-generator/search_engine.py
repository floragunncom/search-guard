from requests.auth import HTTPBasicAuth
import json
import requests

SEARCH_ENGINE_URL = 'https://localhost:9200'
CREDENTIALS_USERNAME = 'admin'
CREDENTIALS_PASSWORD = 'admin'
# CREDENTIALS_PASSWORD = 'syS@os6879?!??.??,?!? ^_^'
# CREDENTIALS_USERNAME = 'elastic'
# CREDENTIALS_PASSWORD = 'oUHso7_ZPo6YK-qE+R3E'
BASIC_AUTH = HTTPBasicAuth(CREDENTIALS_USERNAME, CREDENTIALS_PASSWORD)

OPENSEARCH_BULK_CONTENT_TYPE = 'application/vnd.opensearch+x-ndjson'
ES_BULK_CONTENT_TYPE='application/x-ndjson'
## this should be detected automatically
BULK_CONTENT_TYPE=ES_BULK_CONTENT_TYPE

def create_document(index_name, documents):
    url = f'{SEARCH_ENGINE_URL}/{index_name}/_bulk'
    request_body = ''
    for current_document in documents:
        operation = '{"index":{"_index":"' + index_name + '"}}'
        document = json.dumps(current_document)
        request_body += operation + '\n' + document + '\n'
    request_body += '\n'
    response = requests.post(url, data=request_body, verify=False, auth=BASIC_AUTH, headers={'Content-Type': BULK_CONTENT_TYPE})
    # print(f'Bulk response status code {response.status_code} and body {response.text}')
    if response.status_code != 200: # checking results of bulk request should be more sophisticated
        raise Exception(f'Response status code {response.status_code} and body {response.text}, cannot create measurement')

def send_iot_data(measurements):
    create_document('iot', measurements)

