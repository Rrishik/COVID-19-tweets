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


def connect_to_twitter_Auth():
    auth = tweepy.AppAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return api

def extract_tweets(tweet_object):
    tweet_list =[]
    for tweet in tweet_object:
        id = tweet.id
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
searchQuery = (" OR ").join(hashtags) + " -filter:retweets"

api = connect_to_twitter_Auth()
if (not api):
    print ("Can't Authenticate :(")
    sys.exit(-1)

tweetsPerQry = 100
sinceId = None
max_id = 1256735220293791744


while(1):
    try:
        if (max_id <= 0):
            if (not sinceId):
                new_tweets = api.search(q=searchQuery, count=tweetsPerQry, tweet_mode="extended", lang = 'en')
            else:
                new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                        since_id=sinceId, tweet_mode="extended", lang = 'en')
        else:
            if (not sinceId):
                new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                        max_id=str(max_id - 1), tweet_mode="extended", lang = 'en')
            else:
                new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                        max_id=str(max_id - 1),
                                        since_id=sinceId, tweet_mode="extended", lang = 'en')
        if not new_tweets:
            print("No more tweets found")
            break
        tweets_df = extract_tweets(new_tweets)
        if(path.exists('old_tweets.csv')):
            tweets_df.to_csv('old_tweets.csv', mode='a', index=False, header=None)
        else:
            tweets_df.to_csv('old_tweets.csv', mode='a', index=False)
#        print(tweets_df.iloc[-5:].id)
#        print()
        max_id = tweets_df.iloc[-1].id
    except tweepy.TweepError as e:
        print("some error : " + str(e))
        break
