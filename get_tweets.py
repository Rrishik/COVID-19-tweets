import pandas as pd
import numpy as np
import time
# !pip install tweepy
import tweepy
# !pip install python-twitter
import twitter
import csv
# from kaggle_secrets import UserSecretsClient
from os import path
import config

ACCESS_TOKEN = config.ACCESS_TOKEN
ACCESS_SECRET = config.ACCESS_SECRET
CONSUMER_KEY = config.CONSUMER_KEY
CONSUMER_SECRET = config.CONSUMER_SECRET


def connect_to_twitter_OAuth():
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    api = tweepy.API(auth)
    return api

def extract_tweets(tweet_object):
    tweet_list =[]
    for tweet in tweet_object:
        id = tweet.id_str
        if tweet.place is not None:
            country = tweet.place.country
        else:
            country = None
        text = tweet.full_text
        created_at = tweet.created_at
        tweet_list.append({'id':id, 'created_at':created_at,'text':text, 'country':country})
    df = pd.DataFrame(tweet_list, columns=['id','created_at','text', 'country'])
    return df

hashtags = ["#coronavirus", "#coronavirusoutbreak", "#coronavirusPandemic", "#covid19", "#covid_19", "#epitwitter", "#ihavecorona"]
query = (" OR ").join(hashtags) + " -filter:retweets"


api = connect_to_twitter_OAuth()
id_high = 1258576859773583360
while(1):
    
    search_results = api.search(q=query, count=200, lang='en', tweet_mode="extended", since_id = str(id_high))
    tweets_df = extract_tweets(search_results)
    if(path.exists('tweets.csv')):
        tweets_df.to_csv('tweets.csv', mode='a', index=False, header=None)
    else:
        tweets_df.to_csv('tweets.csv', mode='a', index=False)
    id_high = max(tweets_df['id'])
    time.sleep(12)
