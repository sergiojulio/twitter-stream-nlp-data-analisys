import tweepy
from tweepy import StreamingClient, StreamRule

client = tweepy.StreamingClient("AAAAAAAAAAAAAAAAAAAAANCmfAEAAAAAhnQiOhIy5j7q06RhF8YFw0r726o%3DUM6K22NMhm6Fn1k7CeJJm3Jt5EKqwKkZsLopUUwtdZLcmYw7b3")

print(client.get_rules())