# https://improveandrepeat.com/2022/04/python-friday-117-streaming-search-results-with-tweepy/
import tweepy
from tweepy import StreamingClient, StreamRule
import json
import datetime


class Twitterapi(tweepy.StreamingClient):

    kafka_producer = ''

    #def __init__(self, kafka_producer):
    #    kafka_producer = self.kafka_producer
    # add hash tag
    # remove hash tag
    """
    @staticmethod
    def on_tweet(self, tweet):
        print(f"{tweet.id} {tweet.created_at} ({tweet.author_id}): {tweet.text}")
        print("-"*50)
    """


    def stream(self, hashtag, kafka_producer):

        self.kafka_producer = kafka_producer

        #
        # rule logic
        # print(printer.get_rules())
        # is already init my rule?
        # rule = StreamRule(value=hashtag)
        # yes:
        # printer.add_rules(StreamRule(value="Madrid"))
        # printer.delete_rules([1659153209892429824])
        #

        x = hashtag
        # printer.add_rules(rule)
        print(self.get_rules())

        print("hello")
        # STREAMING LIMIT
        self.filter(tweet_fields="created_at,geo,id,lang,text")
        
    

    @staticmethod
    def test(hashtag):
        return hashtag
    

    def on_data(self, data):
        data = json.loads(data)
        # lang filter here
        if data['data']['lang'] == 'en':
            try:
                topic_name = "trump" # arg - env
                now = datetime.now()
                text = data['data']['text']
                # if loaded
                # self.kafka_producer.send(topic_name, value={'datetime': now, 'text': text})
                # 
                print('succefully sent to brokers')
            except Exception as ex:
                print(str(ex))
            
            print(data)
            print("-"*50)

        return True    
    

    def on_error(self, status):
        print(status)
