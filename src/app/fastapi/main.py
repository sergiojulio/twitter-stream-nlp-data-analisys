from fastapi import FastAPI
from dotenv import load_dotenv
import os
from pathlib import Path
# import kafka
from kafka import KafkaProducer

from src.app.twitter.twitterapi import Twitterapi

from src.app.mastodon.mastodonapi import Mastodonapi

import json
import time,csv
from datetime import datetime

# it should be get from container env
dotenv_path = Path('.venv')
load_dotenv(dotenv_path=dotenv_path)
#
kafka_topic = os.getenv('KAFKA_TOPIC')
# twitter deprecated
bearer_token = os.getenv('BEARER_TOKEN')
stream_souce = os.getenv('STREAM_SOURCE')
# mastodon
access_token = os.getenv('ACCESS_TOKEN')

# init kafka
# >>> producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
kafka_producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), bootstrap_servers='kafka:9093') # kafka:9093
#kafka_producer = ''

app = FastAPI()

@app.get("/streaming_csv")
async def root():
 
    # with open csv
    csvfile = open("/code/src/app/twitter/tweets.csv","r") # ENV STREAM_SOURCE

    reader = csv.DictReader(csvfile)

    for row in reader:

        data = {
            "text": row['text'], # ENV field source
            "datetime": datetime.now()
        }

        data_to_send = json.dumps(data) 

        # send data via producer
        kafka_producer.send(kafka_topic, bytes(data_to_send, encoding='utf-8'))
        
        time.sleep(1) # ENV

    kafka_producer.close()

    return {"message": "finished"}


# twitter deprecated
@app.get("/streaming_twitter")
async def root():

    # init twitter 
    printer = Twitterapi(bearer_token)
    # 
    # init streaming 
    printer.stream(kafka_topic, kafka_producer)

    # init kafka
    return {"message": "finished"}


@app.get("/streaming_mastodon")
async def root():


    printer = Mastodonapi()

    printer.stream(kafka_topic, kafka_producer, access_token)

    # init kafka
    return {"message": "finished"}


@app.get("/token")
async def root():
    return {"token": os.getenv('BEARER_TOKEN')}


@app.get("/test")
async def root():
    producer = KafkaProducer(bootstrap_servers='kafka:9093')  # kafka:9093 kafka debe venir del .env
    producer.send('trump', bytes('hola', encoding='utf-8'))



