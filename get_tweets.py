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


def connectOAuth():
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    api = tweepy.API(auth)
    return api

def extractIds(tweet_object):
    tweet_list =[]
    for tweet in tweet_object:
        id = tweet.id_str
        tweet_list.append({'id':id})
    df = pd.DataFrame(tweet_list, columns=['id'])
    return df

# List of hashtags to track
hashtags = ["#coronavirus", "#coronavirusoutbreak", "#coronavirusPandemic", "#covid19", "#covid_19", "#epitwitter", "#ihavecorona"]

# Building the search query
query = (" OR ").join(hashtags) + " -filter:retweets"

id_high = 1261836344239755264 # Seed id to start searching for tweets after this id(useful for the case when you have to resume getting tweets from a specific id

api = connectOAuth()
# This script runs 24*7 without using much of the system resources

while(1):
    
    search_results = api.search(q = query, count = 100, lang = 'en', since_id = str(id_high))
    tweets_df = extractIds(search_results)
    
    if(path.exists('tweets.csv')):
        tweets_df.to_csv('tweets.csv', mode = 'a', index=False, header=None)
    else:
        tweets_df.to_csv('tweets.csv', mode = 'a', index=False)
    if(not tweets_df.empty):
        id_high = max(tweets_df['id']) # Updating the seed id from which to start searching in the next iteration 
    time.sleep(12)
