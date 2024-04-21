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
    access_token = ''

    def on_update(self, status):

        if words_re.search(status.content) and str(status.language) == "en":

            text = re.sub('<[^<]+?>', '', str(status.content))
            print(re.sub(r'^https?:\/\/.*[\r\n]*', '', text, flags=re.MULTILINE))
            print("=============================================================")
            # KAFKA SEND
            topic_name = "trump" # arg - env
            now = datetime.datetime.now()
            # if loaded
            """
                self.kafka_producer.send(topic_name, value={'datetime': now, 'text': text})
            AttributeError: 'str' object has no attribute 'send'

            """
            #self.kafka_producer.send(topic_name, value={'datetime': now, 'text': text})
            # 
            


class Mastodonapi():

    def stream(self, hashtag, kafka_producer, access_token):

        

        self.kafka_producer = kafka_producer

        mastodon = Mastodon(version_check_mode="none",
                            access_token=access_token, 
                            api_base_url="https://mastodon.social/")
        
        print(mastodon.account_verify_credentials())
        
        mastodon.stream_public(listener=MastodonApiListiner())


#print(mastodon.account_verify_credentials())

#mastodon.stream_hashtag(tag='israel', listener=Listener())
#mastodon.stream_public(listener=Mastodonapi())



