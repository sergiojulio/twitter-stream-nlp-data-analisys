import mastodon
from mastodon import Mastodon
from pprint import pprint
import datetime
import os
from time import gmtime, strftime
import re


# Listener for Mastodon events
class MastodonApiListiner(mastodon.StreamListener):

    kafka_producer = ''
    kafka_topic = ''
    mastodon_key_word_list = ''

    def on_update(self, status):

        #### Build our Regex
        words_re = re.compile("|".join(self.mastodon_key_word_list.split(",")), re.IGNORECASE)

        # only english language
        if words_re.search(status.content) and str(status.language) == "en":

            # remove html tags
            text = self.clean_post(status.content)
            
            # for debug purposes
            print(text)
            
            kafka_topic = self.kafka_topic 

            # Use a standard date format instead, preferably ISO-8601. 2015-09-01T16:34:02
            now = datetime.datetime.now().replace(microsecond=0).isoformat()
            
            # dont send text empty
            if text:
                self.send(kafka_topic, {'created': str(now), 'text': text})
            # 


    def send(self, kafka_topic, value):
        self.kafka_producer.send(kafka_topic, value)


    def kafka(self, kafka_producer, kafka_topic,mastodon_key_word_list):
        self.kafka_producer = kafka_producer
        self.kafka_topic = kafka_topic
        self.mastodon_key_word_list = mastodon_key_word_list


    def clean_post(self, post):
        stopwords = ["for", "on", "an", "a", "of", "and", "in", "the", "to", "from", "#"]
        temp = post.lower()
        temp = re.sub("'", "", temp) 
        temp = re.sub("@[A-Za-z0-9_]+","", temp)
        #temp = re.sub("#[A-Za-z0-9_]+","", temp)
        temp = re.sub('<[^<]+?>', '', str(temp))
        temp = re.sub(r'http\S+', '', temp)
        temp = re.sub('[()!?]', ' ', temp)
        temp = re.sub('\[.*?\]',' ', temp)
        temp = re.sub("[^a-z0-9]"," ", temp)
        temp = temp.split()
        temp = [w for w in temp if not w in stopwords]
        temp = " ".join(word for word in temp)
        return temp


class Mastodonapi():

    def stream(self, kafka_topic, kafka_producer, access_token, mastodon_key_word_list):

        self.kafka_producer = kafka_producer
        self.kafka_topic = kafka_topic
        self.mastodon_key_word_list = mastodon_key_word_list

        mastodon = Mastodon(version_check_mode="none",
                            access_token=access_token, 
                            api_base_url="https://mastodon.social/")
        
        # print(mastodon.account_verify_credentials())
        listener = MastodonApiListiner()
        listener.kafka(kafka_producer, kafka_topic, mastodon_key_word_list)
        mastodon.stream_public(listener=listener)




