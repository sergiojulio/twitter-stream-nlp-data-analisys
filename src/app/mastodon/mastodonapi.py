import mastodon
from mastodon import Mastodon
from pprint import pprint
import datetime
import os
from time import gmtime, strftime
import re


#### Keywords to match ENV
keywordList = ['usa','ukraine','russia','israel','nato']

#### Build our Regex
words_re = re.compile("|".join(keywordList), re.IGNORECASE)

#### Listener for Mastodon events
class MastodonApiListiner(mastodon.StreamListener):

    kafka_producer = ''
    kafka_topic = ''

    def on_update(self, status):

        if words_re.search(status.content) and str(status.language) == "en":

            # remove html tags
            text = re.sub('<[^<]+?>', '', str(status.content))
            #
            print(re.sub(r'^https?:\/\/.*[\r\n]*', '', text, flags=re.MULTILINE))
            print("=============================================================")
            
            kafka_topic = self.kafka_topic # arg - env
            # 2024-04-28 00:05:09.306463
            # Use a standard date format instead, preferably ISO-8601. 2015-09-01T16:34:02
            # now = datetime.datetime.now().replace(microsecond=0)
            now = datetime.datetime.now().replace(microsecond=0).isoformat()
            # now = "2015-09-01T16:34:02"

            # KAFKA SEND
            # is kafka running?
            self.send(kafka_topic, {'created': str(now), 'text': text})
            # 

    def send(self, kafka_topic, value):
        self.kafka_producer.send(kafka_topic, value)

    def kafka(self, kafka_producer, kafka_topic):
        self.kafka_producer = kafka_producer
        self.kafka_topic = kafka_topic


class Mastodonapi():

    def stream(self, kafka_topic, kafka_producer, access_token):

        # print(kafka_producer)

        self.kafka_producer = kafka_producer
        self.kafka_topic = kafka_topic

        mastodon = Mastodon(version_check_mode="none",
                            access_token=access_token, 
                            api_base_url="https://mastodon.social/")
        
        print(mastodon.account_verify_credentials())

        listener = MastodonApiListiner()
        listener.kafka(kafka_producer, kafka_topic)

        mastodon.stream_public(listener=listener)


#print(mastodon.account_verify_credentials())

#mastodon.stream_hashtag(tag='israel', listener=Listener())
#mastodon.stream_public(listener=Mastodonapi())



