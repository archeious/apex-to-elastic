from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import json
from dotenv import load_dotenv
import os
from pathlib import Path
import pprint
import requests
import xmltodict


basepath = Path()
basedir = str(basepath.cwd())
# Load the environment variables
envars = basepath.cwd() / '.env'
print("Loading config from " + str(envars))
load_dotenv(envars)

base_url      = os.environ.get("apex-url")
elastic_url   = os.environ.get("elastic-url")
elastic_index = os.environ.get("elastic-index")

es = Elasticsearch(elastic_url)

result = es.search(
    index=elastic_index,
    body={
        "sort": [
            {
            "timestamp": {
                    "order": "desc"
                }
            }
        ],
        "size": 1
    }
)

now = datetime.strptime(result["hits"]["hits"][0]["_source"]["timestamp"].replace(':', ''), "%Y-%m-%dT%H%M%S%z" ) + timedelta(minutes=1)

url = base_url + '/cgi-bin/datalog.xml?sdate=' + now.strftime("%y%m%d%H%M")
print("Connecting to " + url)
response = requests.get(url)
data = xmltodict.parse(response.content)


print(now.astimezone().isoformat())

records = data["datalog"]["record"]
print ( "Processing " + str(len(records)) )

for record in records: 
    es_doc = {}
    es_doc["probes"] = {}


    es_doc['timestamp'] = datetime.strptime(record["date"],"%m/%d/%Y %H:%M:%S").astimezone().isoformat();

    for probe in record["probe"]:
        es_doc['probes'][probe['name']] = float(probe['value'])

    print(json.dumps(es_doc))

    resp = es.index(index=elastic_index, document=es_doc)
    print(resp['result'])
    
