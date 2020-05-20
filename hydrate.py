import pandas as pd
import os
from twarc import Twarc
import config # A python file which contains your Twitter API Credentials
import time
from datetime import date, timedelta

# This script was scheduled to run daily, so the filenames to be processed was yesterday's date
filename = (date.today() - timedelta(days = 1)).strftime("%m-%d-%Y")

# Main directory which contails the ids folder and the full data folder
main_dir = './'
ids_dir = main_dir + 'data/'
# Make sure you create this folder in the main directory before running this script
target_dir = main_dir + 'data_full/'

# Twitter API Credentials
ACCESS_TOKEN = config.ACCESS_TOKEN
ACCESS_SECRET = config.ACCESS_SECRET
CONSUMER_KEY = config.CONSUMER_KEY
CONSUMER_SECRET = config.CONSUMER_SECRET

t = Twarc(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

tweet_ids = pd.read_csv(ids_dir + filename + ".csv", lineterminator = '\n')
tweet_objects = []

for tweet in t.hydrate(tweet_ids.id.drop_duplicates()):
    tweet_objects.append(tweet)
    
df_full = pd.DataFrame(tweet_objects, columns = ['created_at', 'id', 'id_str', 'full_text', 'truncated', 'display_text_range', 'entities', 'source', 'in_reply_to_status_id', 'in_reply_to_status_id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str', 'in_reply_to_screen_name', 'user', 'geo', 'coordinates', 'place', 'contributors', 'is_quote_status', 'retweet_count', 'favorite_count', 'favorited', 'retweeted', 'possibly_sensitive', 'lang'])
df_full.dropna(subset = ['id']).to_csv(target_dir + filename + '_full.csv', index = None)
