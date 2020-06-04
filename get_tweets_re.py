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

hashtags = ["#coronavirus", "#coronavirusoutbreak", "#coronavirusPandemic", "#covid19", "#covid_19", "#epitwitter", "#ihavecorona", '#pandemic', "#covid__19"]
query = (" OR ").join(hashtags)

# 99999999999999999999
max_id = 1267969244127301632


# This script was scheduled to run daily, so the filenames to be processed was yesterday's date
# today = date.today().strftime("%Y-%m-%d")
yesterday = (date.today() - timedelta(days = 1)).strftime("%Y-%m-%d")

filename = (date.today() - timedelta(days = 1)).strftime("%m-%d-%Y")

api = connectAuth()

full_df = pd.DataFrame(columns = ['created_at', 'id', 'id_str', 'full_text', 'truncated', 'display_text_range', 'entities', 'source', 'in_reply_to_status_id', 'in_reply_to_status_id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str', 'in_reply_to_screen_name', 'user', 'geo', 'coordinates', 'place', 'contributors', 'is_quote_status', 'retweet_count', 'favorite_count', 'favorited', 'retweeted', 'possibly_sensitive', 'lang'])

while(1):
    try:
        new_tweets = api.search(q = query, count = 100, max_id = str(max_id - 1), tweet_mode="extended", lang = 'en')
        
        if not new_tweets:
            print("No more tweets found")
            break
        
        new_full_df = extractTweet(new_tweets)
        print(new_full_df.iloc[0])
        print()
        full_df = full_df.append(new_full_df)
        
        max_id = new_full_df.iloc[-1].id
        
        if pd.to_datetime(pd.to_datetime(new_full_df.iloc[-1].created_at).strftime("%Y-%m-%d %H:%M:%S")) < pd.to_datetime(yesterday + ' 00:00:00'):
            print("Tweet extraction complete!")
            print("Saving data. . .")
            full_df.to_csv(main_dir + 'data_retweets/' + filename + '.csv', index = None)
            break
#         time.sleep(5)
        
    except tweepy.TweepError as e:
        print("Tweepy error! : " + str(e))
        break
