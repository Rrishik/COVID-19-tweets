import pandas as pd
import numpy as np
import time
# !pip install tweepy
import tweepy
# !pip install python-twitter
import twitter
import csv
from os import path
import config

ACCESS_TOKEN = config.ACCESS_TOKEN
ACCESS_SECRET = config.ACCESS_SECRET
CONSUMER_KEY = config.CONSUMER_KEY
CONSUMER_SECRET = config.CONSUMER_SECRET


def connectAuth():
    auth = tweepy.AppAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return api

def extractIds(tweet_object):
    tweet_list =[]
    for tweet in tweet_object:
        id = tweet.id
        tweet_list.append({'id':id})
    df = pd.DataFrame(tweet_list, columns=['id'])
    return df

hashtags = ["#coronavirus", "#coronavirusoutbreak", "#coronavirusPandemic", "#covid19", "#covid_19", "#epitwitter", "#ihavecorona"]
query = (" OR ").join(hashtags) + " -filter:retweets"


sinceId = None
max_id = 1258641165982806019

api = connectAuth()
while(1):
    try:
        if (max_id <= 0):
            if (not sinceId):
                new_tweets = api.search(q = query, count = 100, lang = 'en')
            else:
                new_tweets = api.search(q = query, count = 100, since_id = sinceId, lang = 'en')
        else:
            if (not sinceId):
                new_tweets = api.search(q = query, count = 100, max_id = str(max_id - 1), lang = 'en')
            else:
                new_tweets = api.search(q = query, count = 100, max_id = str(max_id - 1), since_id = sinceId, lang = 'en')
        
        if not new_tweets:
            print("No more tweets found")
            break
        tweets_df = extractIds(new_tweets)
        
        if(path.exists('old_tweets.csv')):
            tweets_df.to_csv('old_tweets.csv', mode='a', index=False, header=None)
        else:
            tweets_df.to_csv('old_tweets.csv', mode='a', index=False)

        max_id = tweets_df.iloc[-1].id
        
    except tweepy.TweepError as e:
        print("some error : " + str(e))
        break
