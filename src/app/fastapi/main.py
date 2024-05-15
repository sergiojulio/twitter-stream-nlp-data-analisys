import os
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI
from kafka import KafkaProducer
# twitter deprecated
from src.app.twitter.twitterapi import Twitterapi
from src.app.mastodon.mastodonapi import Mastodonapi

import json
import time, csv
from datetime import datetime

kafka_topic = os.environ["KAFKA_TOPIC"]
kafka_server = os.environ["KAFKA_SERVER"]
mastodon_key_word_list = os.environ["MASTODON_KEY_WORD_LIST"]

# it should be get from container env
dotenv_path = Path('.venv')
load_dotenv(dotenv_path=dotenv_path)
#

# twitter deprecated
bearer_token = os.getenv('BEARER_TOKEN')
stream_source = os.getenv('STREAM_SOURCE')
# mastodon
access_token = os.getenv('ACCESS_TOKEN')


kafka_producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), 
                                bootstrap_servers=kafka_server) 

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
        kafka_producer.send(kafka_topic, bytes(data_to_send, encoding='utf-8'))
        time.sleep(1)

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
    printer.stream(kafka_topic, kafka_producer, access_token, mastodon_key_word_list)
    return {"message": "finished"}


@app.get("/test")
async def root():
    producer = KafkaProducer(bootstrap_servers=kafka_server)
    producer.send(kafka_topic, bytes('Test', encoding='utf-8'))
    return {"kafka_topic":kafka_topic}



