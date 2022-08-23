KEY="MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCxvBUb0DhwEwkksqI8PJfGLiyP2xSUGwb70jnikZ0jTTNRI1DGdWwAbx9fYXIAByYXssZOX6eLARhfvI20LYM/fF5IfFz9JkHRE9QMao1UgjV+z5+9MN6JHc41E/I5aHijeL6nTKfyFXlhCk/ZbZSuDVtESvoOKt5tcazrOesHqI0RHmcPzPAs3W1E/SVZ5apqls858cjm0W9p9M9ifzZr8nxA/avJshEQ8hEfP86ZvIUb3M/qKNdAktFgjCue7ESTJviua3xxtWcI9z9LIfbHw1Hnl/Cv4ASbQY5nXGD5DG3W0MlWq+5mom6eeehVlnGssjqR3y9wI7ub/z7ZCqlRAgMBAAECggEAFuj7YJUvvTSa9F3JX1HhL4TSri1rgubT+OBhoUSrWHpST9ZpSlem9wxb4yfUsc+6F4puGPqoBlk7CtYrfurx9NxDa/0J4IDOsZRofDw84QSSwDijqteyi8Kpirp6Oe+vQ0UkcDzHlkMx3PIfFlQTevcSSWSPxIU+nCVvyHd0Bg26slhAQbZhKZWvXkfOY4yOHPzhcarYD2s+Q26iBcYOH38B9IAyT4moTZkS+or0kcwdE1ZHrhylaGAomVD/ZMPMzd+CtCsGTMvUXAuOgzBYsER3sQa/3J9LYWuRZ3pzxscDW0xIVcMoUYD/mbKfXi1UII3sUR1vAhgdX9CBMjdMpQKBgQD0+qU66NCi7t4fgsxvcva7bFKOOrI41MCaTYadCrENwU0ZTNTMAyYHLcTkyE0DO9gY5lXp9zlfC8Mv1G08VuA+oQ0hcx7BmIsY39wPFtQMZOlzl5E+T0XHPDR2MiYPehLDZA0rbyWAhvOTIhc/ODP6JnRFzU+5W+vEnpN9fXUVRQKBgQC5uwCkol0mOCkriAQ/40iNcY7fLp6oPNC94LZOZGELjO/IvpAE7b0RT8wKCiUmB0kiUnD3GrEeXqDjVJIg8v49jFSwSxLVzx+HDWskuhH2hPfyQLWVihtIBjGrTT1umoFTKd8TYj+o1S5CbhFOxvtwgUc5tzYk7ooepQ7vHhkGnQKBgEqHXnE3lxGanhT0FAHr9cg7Qjpm/QVxJE9NOqDYOdk3b5880phmdNFGSVpY3aUYNbwNhyGwxtF1oKISfFEZFQu4r2f3v+mh4N9ma2pjxYsnwCYcfGF6eH4OgN9cjluzBbZP3/nQzJX3eG7QtkXTcWyu+jyqI5D+uBGPNMu+uToJAoGAD8bZzCJapUd5/8+jBMZKwHEYAM9V/NaFqMtw0QHn2HJVYAkH9NM5D0JnA6dO9ocB6F92ZxcmWn0RT548d34MqK/F9d+6rtzUQcWbB1ii8/zhjvt+MUC1Bo44I+QAxudq+uSApYXgAHhzYIM3BykR7MGeikGM4OA+bVH6DcfRumUCgYBTz+N7Dya9toExylJlI80cGznqHJ6xy3Bl7fr+NwO/IsN/hu4PLAj8/t6Dpu1D1kLeNPpc2ykmNUbf9X0/ZkhXrhYjbNzzlBv7N40Q0kHqXVDfpBpuP84qbHKjhfoFBcbjfLRkjRkcZ+TmtM1GYUEEv/8fsYFyK9kvCiUD4PMU8w=="

curl -Ssk -u admin:admin -XDELETE "https://localhost:9200/enc_index_1?pretty"
echo
curl -Ssk -u admin:admin -XPUT "https://localhost:9200/enc_index_1?pretty" -H 'content-type: application/json' -d '

{
  "settings": {
    "encryption_enabled": true,
    "encryption_key": "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsbwVG9A4cBMJJLKiPDyXxi4sj9sUlBsG+9I54pGdI00zUSNQxnVsAG8fX2FyAAcmF7LGTl+niwEYX7yNtC2DP3xeSHxc/SZB0RPUDGqNVII1fs+fvTDeiR3ONRPyOWh4o3i+p0yn8hV5YQpP2W2Urg1bREr6DirebXGs6znrB6iNER5nD8zwLN1tRP0lWeWqapbPOfHI5tFvafTPYn82a/J8QP2rybIREPIRHz/OmbyFG9zP6ijXQJLRYIwrnuxEkyb4rmt8cbVnCPc/SyH2x8NR55fwr+AEm0GOZ1xg+Qxt1tDJVqvuZqJunnnoVZZxrLI6kd8vcCO7m/8+2QqpUQIDAQAB",
    
    "number_of_shards": 3,
    "number_of_replicas": 1,
    "store.type": "fs",
    "translog.durability": "async",
    "translog.sync_interval": "800s",
    "analysis": {
      "analyzer": {
        "default": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "blind_hash"

          ]
        }
      }
    }
  },
  "mappings": {
  
      "properties": {
        "full_name": {
          "type": "text",
          "fielddata": true,
          "index_options": "offsets",
          "norms": true,
          "term_vector": "with_positions_offsets_payloads"
        },
        "credit_card_number": {
          "type": "keyword",
          "doc_values": false
        },
        "age": {
          "type": "integer",
          "store": true
        },
        "remarks": {
          "type": "text",
          "store": true,
          "index": false,
          "doc_values": false
        }
      }
    
  }
}
'
echo
curl -Ssk -u admin:admin -XPOST "https://localhost:9200/enc_index_1/_doc?refresh=false" -H 'content-type: application/json' -H "x-osec-pk:$KEY" -d '

{
    "full_name":"Captain Kirk",
    "credit_card_number":"1234",
    "age":45,
    "remarks":"take care"
}

'
echo

curl -Ssk -XPOST https://localhost:9200/en*/_search?pretty -H 'content-type: application/json' -u admin:admin -d '
{
  "aggs": {
    "aggi": {
      "terms": { "field": "full_name" }
    }
  }
 }

'

echo

curl -Ssk -XPOST https://localhost:9200/en*/_search?pretty -H 'content-type: application/json' -H "x-osec-pk:$KEY" -u admin:admin -d '
{
  "aggs": {
    "aggi": {
      "terms": { "field": "full_name" }
    }
  }
 }

'
