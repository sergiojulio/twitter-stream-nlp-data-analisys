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
import datetime

kafka_topic = os.environ["KAFKA_TOPIC"]
kafka_server = os.environ["KAFKA_SERVER"]
mastodon_key_word_list = os.environ["MASTODON_KEY_WORD_LIST"]

# it should be get from container env
dotenv_path = Path('.venv')
load_dotenv(dotenv_path=dotenv_path)

# twitter deprecated
bearer_token = os.getenv('BEARER_TOKEN')
stream_source = os.getenv('STREAM_SOURCE')
# mastodon
access_token = os.getenv('ACCESS_TOKEN')


app = FastAPI()

@app.get("/streaming_csv")
async def root():

    parent_dir_path = os.path.dirname(os.path.realpath(__file__))
    kafka_producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), 
                                bootstrap_servers=kafka_server) 
 
    # with open csv
    csvfile = open(parent_dir_path + "/../twitter/tweets.csv","r") 
    reader = csv.DictReader(csvfile)

    for row in reader:
        now = datetime.datetime.now().replace(microsecond=0).isoformat()
        kafka_producer.send(kafka_topic, {'created': str(now), 'text': row['text']})
        time.sleep(5)

    kafka_producer.close()
    return {"message": "finished"}


# twitter deprecated
@app.get("/streaming_twitter")
async def root():
    
    kafka_producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), 
                                bootstrap_servers=kafka_server) 
    # init twitter 
    printer = Twitterapi(bearer_token)
    # 
    # init streaming 
    printer.stream(kafka_topic, kafka_producer)

    # init kafka
    return {"message": "finished"}


@app.get("/streaming_mastodon")
async def root():
    kafka_producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), 
                                bootstrap_servers=kafka_server) 
    
    printer = Mastodonapi()
    printer.stream(kafka_topic, kafka_producer, access_token, mastodon_key_word_list)
    return {"message": "finished"}


@app.get("/test")
async def root():
    producer = KafkaProducer(bootstrap_servers=kafka_server)
    producer.send(kafka_topic, bytes('Test', encoding='utf-8'))
    return {"kafka_topic":kafka_topic}



