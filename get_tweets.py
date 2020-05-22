import pandas as pd
import numpy as np
import time
# !pip install tweepy
import tweepy
# !pip install python-twitter
import twitter
import csv
from os import path
import config2 as config
from datetime import date, timedelta

ACCESS_TOKEN = config.ACCESS_TOKEN
ACCESS_SECRET = config.ACCESS_SECRET
CONSUMER_KEY = config.CONSUMER_KEY
CONSUMER_SECRET = config.CONSUMER_SECRET

main_dir = '/home/vca_rishik/rishik/COVID-19-tweets/'

def connectAuth():
    auth = tweepy.AppAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return api

def extractTweet(tweets):
    tweet_list = []
    for tweet in tweets:
        tweet_list.append(tweet._json)
    return pd.DataFrame(tweet_list, columns = ['created_at', 'id', 'id_str', 'full_text', 'truncated', 'display_text_range', 'entities', 'source', 'in_reply_to_status_id', 'in_reply_to_status_id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str', 'in_reply_to_screen_name', 'user', 'geo', 'coordinates', 'place', 'contributors', 'is_quote_status', 'retweet_count', 'favorite_count', 'favorited', 'retweeted', 'possibly_sensitive', 'lang'])

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
max_id = 1263620612557242369


# This script was scheduled to run daily, so the filenames to be processed was yesterday's date
# today = date.today().strftime("%Y-%m-%d")
yesterday = (date.today() - timedelta(days = 1)).strftime("%Y-%m-%d")

filename = (date.today() - timedelta(days = 1)).strftime("%m-%d-%Y")

api = connectAuth()

id_df = pd.DataFrame(columns = ['id', 'created_at'])
full_df = pd.DataFrame(columns = ['created_at', 'id', 'id_str', 'full_text', 'truncated', 'display_text_range', 'entities', 'source', 'in_reply_to_status_id', 'in_reply_to_status_id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str', 'in_reply_to_screen_name', 'user', 'geo', 'coordinates', 'place', 'contributors', 'is_quote_status', 'retweet_count', 'favorite_count', 'favorited', 'retweeted', 'possibly_sensitive', 'lang'])

while(1):
    try:
        new_tweets = api.search(q = query, count = 100, max_id = str(max_id - 1), tweet_mode="extended", lang = 'en')
        
        if not new_tweets:
            print("No more tweets found")
            break
        
        new_full_df = extractTweet(new_tweets)
        new_id_df = extractIds(new_tweets)
        print(new_id_df.head())
        print()
        id_df = id_df.append(new_id_df)
        full_df = full_df.append(new_full_df)
        
        max_id = new_full_df.iloc[-1].id
        
        if new_id_df.iloc[-1].created_at < pd.to_datetime(yesterday + ' 00:00:00'):
            print("Tweet extraction complete!")
            print("Saving data. . .")
            id_df.sort_values(by = ['id']).to_csv(main_dir + 'data/' + filename + '.csv', index = None)
            full_df.to_csv(main_dir + 'data_full/' + filename + '_full.csv', index = None)
            break
        
    except tweepy.TweepError as e:
        print("Tweepy error! : " + str(e))
        break
