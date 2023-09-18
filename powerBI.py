import requests
import json
import pandas as pd
from datetime import datetime
import time
from kafka import KafkaConsumer, KafkaClient

API_ENDPOINT = "https://api.powerbi.com/beta/5e158e2a-0596-4a6c-8801-3502aef4563f/datasets/fbdbca55-4147-4952-92aa-fe3d20f3821d/rows?redirectedFromSignup=1%2C1&key=bCIGTg8qGVtArWt9NsyGgU0GIiZANdGdEvxmtK0vzCpoAJiMh%2BZgw69AIvPDRztZqgCopWqoeUcRrOYXMGmkkA%3D%3D"
TOPIC = "powerBI"    
#some statistical variable
GLOBAL_TOTAL = 0
start_timestamp = last_timestamp = time.time()


def send2API(msg):
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.request(
        method="POST",
        url=API_ENDPOINT,
        headers=headers,
        data=bytes(json.dumps(msg).encode('utf-8'))
    )
    print(response)
    print(msg)
    
    

bootstrap_servers = ['localhost:9092']

if __name__ == "__main__":
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')
    for data_json in consumer:
        msg = data_json.value.decode("utf-8")
        msg = json.loads(msg)
        send2API(msg)

        