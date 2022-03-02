from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import json
from dotenv import load_dotenv
import os
import pprint
import requests
import xmltodict


load_dotenv()
base_url      = os.environ.get("apex-url")
elastic_url   = os.environ.get("elastic-url")
elastic_index = os.environ.get("elastic-index")

now = datetime.now() - timedelta(minutes=1)

url = base_url + '/cgi-bin/datalog.xml?sdate=' + now.strftime("%y%m%d%H%M")
response = requests.get(url)
data = xmltodict.parse(response.content)
data["datalog"]["record"][0]["timestamp"] = now.astimezone().isoformat()

es_doc = {}
es_doc["probes"] = {}

es_doc['timestamp'] = now.astimezone().isoformat()

for probe in data["datalog"]["record"][0]["probe"]:
    es_doc['probes'][probe['name']] = float(probe['value'])

print(json.dumps(es_doc))

es = Elasticsearch(elastic_url)
resp = es.index(index=elastic_index, document=es_doc)
print(resp['result'])