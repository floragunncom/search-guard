
## 1. Create encrypted Index ##
$ enctl create-encrypted-index \
   --index my_encrypted_index \
   --host opensearch.company.com \
   --port 9200 \
   --user myuser

Encrypted index "my_encrypted_index" created
Keys are stored in keys/my_encrypted_index/

## 2. Generate header ##
$ enctl generate-header \
   --index my_encrypted_index \
   --host opensearch.company.com \
   --port 9200 \
   --user myuser

Header is "x-osei-pk: MIIabcDefhjkh76hlKJh61dFGTqhKI...cKKm8Xg"

## 3. Index encrypted data ##
$ curl -X POST "https://opensearch.company.com:9200/my_encrypted_index/_doc" \
       -H 'x-osei-pk: MIIabcDefhjkh76hlKJh61dFGTqhKI...cKKm8Xg' \
       -H 'Content-Type: application/json' \
       -d '{
            "customer_name": "John Foo",
            "customer_address": "Mulholland Drive",
            "rating": "AAA"
           }'

## 4. Search encrypted data
$ curl -X GET "https://opensearch.company.com:9200/my_encrypted_index/_doc" \
       -H 'x-osei-pk: MIIabcDefhjkh76hlKJh61dFGTqhKI...cKKm8Xg' \
       -H 'Content-Type: application/json' \
       -d '{
             "query": {
               "wildcard": {
                 "customer_name": {
                   "value": "Joh*"
                 }
               }
             }
           }'


  "hits": {
    "total": {
      "value": 2,
      "relation": "eq"
    },
    "max_score": 1.4128811,
    "hits": [
      {
        "_index": "my_encrypted_index",
        "_id": "YiolopmKJl9182",
        "_score": 1.3762962,
        "_source": {
           "customer_name": "John Foo",
           "customer_address": "Mulholland Drive",
           "rating": "AAA"
        },
        ...

## Index „my_encrypted_index“ ##
  {
    "customer_name": "Gt8bVe1ZcA4nkUJujQiqYw==",
    "customer_address": "Ux0yVnBIuBEbMkTod1yK1w==",
    "rating": "1c72pYsCrWRzj/HhUxcRvA=="
  }