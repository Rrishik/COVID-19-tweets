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
from datetime import date, timedelta

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
        created_at = tweet.created_at
        tweet_list.append({'id':id, 'created_at':created_at})
    df = pd.DataFrame(tweet_list, columns=['id', 'created_at'])
    return df

hashtags = ["#coronavirus", "#coronavirusoutbreak", "#coronavirusPandemic", "#covid19", "#covid_19", "#epitwitter", "#ihavecorona"]
query = (" OR ").join(hashtags) + " -filter:retweets"

# 99999999999999999999
max_id = 99999999999999999999


# This script was scheduled to run daily, so the filenames to be processed was yesterday's date
# today = date.today().strftime("%Y-%m-%d")
yesterday = (date.today() - timedelta(days = 1)).strftime("%Y-%m-%d")

filename = (date.today() - timedelta(days = 1)).strftime("%m-%d-%Y")

api = connectAuth()
df = pd.DataFrame(columns = ['id', 'created_at'])

while(1):
    try:
        new_tweets = api.search(q = query, count = 100, max_id = str(max_id - 1), lang = 'en')
        
        if not new_tweets:
            print("No more tweets found")
            break
        tweets_df = extractIds(new_tweets)
        print(tweets_df.head())
        print()
        df = df.append(tweets_df)
        
        max_id = tweets_df.iloc[-1].id
        if tweets_df.iloc[-1].created_at < pd.to_datetime(yesterday + ' 00:00:00'):
            print("Tweet extraction complete!")
            df.sort_values(by = ['id']).to_csv('./data/' + filename + '.csv')
            break
        
    except tweepy.TweepError as e:
        print("Tweepy error! : " + str(e))
        break
