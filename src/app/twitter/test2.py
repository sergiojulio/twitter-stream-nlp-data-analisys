import mastodon
from mastodon import Mastodon
from pprint import pprint
import requests

#from bs4 import BeautifulSoup
#import pulsar
#from pulsar.schema import *

import time
import sys
import datetime
import subprocess
import sys
import os
from subprocess import PIPE, Popen
import traceback
import math
import base64
import json
from time import gmtime, strftime
import random, string

#import psutil

import uuid
import json
import socket 
import logging

#from jsonpath_ng import jsonpath, parse

import re

from dotenv import load_dotenv
import os
from pathlib import Path
# it should be get from container env
dotenv_path = Path('.venv')
load_dotenv(dotenv_path=dotenv_path)
#
access_token = os.getenv('ACCESS_TOKEN')


#### Apache Pulsar
#pulsarClient = pulsar.Client('pulsar://localhost:6650')


#### Keywords to match
keywordList = ['usa','ukraine','russia','israel','nato']

#### Build our Regex
words_re = re.compile("|".join(keywordList), re.IGNORECASE)

#### Listener for Mastodon events

class Listener(mastodon.StreamListener):

 def on_update(self, status):

    #print(status)

    if words_re.search(status.content) and str(status.language) == "en":

        #pulsarProducer = pulsarClient.create_producer(
        #   topic='persistent://public/default/mastodon',
        #    schema = JsonSchema(mastodondata), 
        #    properties={"producer-name": "mastodon-py-strean","producer-id": "mastodon-producer" }
        #    )
        


        uuid_key = '{0}_{1}'.format(strftime("%Y%m%d%H%M%S",gmtime()),uuid.uuid4())

        """
        mastodonRec.language = status.language
        mastodonRec.created_at = str(status.created_at)
        mastodonRec.ts = float(strftime("%Y%m%d%H%M%S",gmtime()))
        mastodonRec.uuid = uuid_key
        mastodonRec.uri = status.uri
        mastodonRec.url = status.url
        mastodonRec.favourites_count = status.favourites_count
        mastodonRec.replies_count = status.replies_count
        mastodonRec.reblogs_count = status.reblogs_count
        mastodonRec.content = status.content 
        mastodonRec.username = status.account.username
        mastodonRec.accountname = status.account.acct
        mastodonRec.displayname = status.account.display_name
        mastodonRec.note = status.account.note
        mastodonRec.followers_count = status.account.followers_count
        mastodonRec.statuses_count = status.account.statuses_count
        """

        text = re.sub('<[^<]+?>', '', str(status.content))
        print(re.sub(r'^https?:\/\/.*[\r\n]*', '', text, flags=re.MULTILINE))
        print("=============================================================")
        
        #pulsarProducer.send(mastodonRec,partition_key=str(uuid_key))
        #pulsarProducer.flush()
        #producer.send('rp4-kafka-1', mastodonRec.encode('utf-8'))
        #producer.flush()

 def on_notification(self, notification):
    # print(f"on_notification: {notification}")
    print("notification")


#curl https://streaming.mastodon.social/api/v1/streaming/public?access_token=ts4X0TcSb3BMDMmTG8_77HOKEL-vi4sLaYbBPeeiYdw

mastodon = Mastodon(version_check_mode="none",
                    access_token=access_token, 
                    api_base_url="https://mastodon.social/")


print(mastodon.account_verify_credentials())

#mastodon = Mastodon(api_base_url='https://streaming.mastodon.social/api/v1/streaming/public?access_token=ts4X0TcSb3BMDMmTG8_77HOKEL-vi4sLaYbBPeeiYdw')
#mastodon.stream_hashtag(tag='israel', listener=Listener())

mastodon.stream_public(listener=Listener())



