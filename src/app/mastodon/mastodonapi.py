import mastodon
from mastodon import Mastodon
from pprint import pprint
import datetime
import os
from time import gmtime, strftime
import re


#### Keywords to match
keywordList = ['usa','ukraine','russia','israel','nato']

#### Build our Regex
words_re = re.compile("|".join(keywordList), re.IGNORECASE)

#### Listener for Mastodon events
class MastodonApiListiner(mastodon.StreamListener):

    kafka_producer = ''

    def on_update(self, status):

        if words_re.search(status.content) and str(status.language) == "en":

            text = re.sub('<[^<]+?>', '', str(status.content))
            print(re.sub(r'^https?:\/\/.*[\r\n]*', '', text, flags=re.MULTILINE))
            print("=============================================================")
            # KAFKA SEND
            topic_name = "trump" # arg - env
            # 2024-04-28 00:05:09.306463
            # Use a standard date format instead, preferably ISO-8601. 2015-09-01T16:34:02
            now = datetime.datetime.now().replace(microsecond=0)
            now = "2015-09-01T16:34:02"
            # if loaded
            """
                self.kafka_producer.send(topic_name, value={'created': now, 'text': text})
            AttributeError: 'str' object has no attribute 'send'

            """
            #bytes('hola', encoding='utf-8')
            self.send(topic_name, {'created': str(now), 'text': text})
            # 
    def send(self, topic_name, value):
        self.kafka_producer.send(topic_name, value)

    def kafka(self, kafka_producer):
        self.kafka_producer = kafka_producer



class Mastodonapi():

    def stream(self, hashtag, kafka_producer, access_token):

        print(kafka_producer)

        self.kafka_producer = kafka_producer

        mastodon = Mastodon(version_check_mode="none",
                            access_token=access_token, 
                            api_base_url="https://mastodon.social/")
        
        print(mastodon.account_verify_credentials())

        clase = MastodonApiListiner()
        clase.kafka(kafka_producer)

        mastodon.stream_public(listener=clase)


#print(mastodon.account_verify_credentials())

#mastodon.stream_hashtag(tag='israel', listener=Listener())
#mastodon.stream_public(listener=Mastodonapi())



