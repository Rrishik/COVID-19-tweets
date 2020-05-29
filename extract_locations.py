import pandas as pd
import carmen
from datetime import date, timedelta
from os import path

resolver = carmen.get_resolver()
resolver.load_locations()


def getLocation(t):
    fields = ['entities', 'user', 'place', 'coordinates']
    for field in fields:
        if(pd.isna(t[field]) or t[field].isnumeric() or (t[field] == 'True' or t[field] == 'False')):
            t[field] = dict()
        else:
            t[field] = eval(t[field])
    
    location = resolver.resolve_tweet(t)
    return location


def getCountry(t):
    loc = getLocation(t)
    if(loc):
        return loc[1].country
    return None

full_dir = '/home/vca_rishik/rishik/COVID-19-tweets/data_full/'
target_dir = full_dir + 'locations/'

filename = (date.today() - timedelta(days = 1)).strftime("%m-%d-%Y")

print("Reading file: " + filename + '_full.csv ... .  .')
df = pd.read_csv(full_dir + filename + '_full.csv')
print('done!')

print()

print("Extracting locations ... ")
df['country'] = df[['entities', 'user', 'place', 'coordinates']].apply(getCountry, axis = 1)
df[['id', 'country']].to_csv(target_dir + filename + '_loc.csv', index = None)
print("done! saved to " + target_dir + filename + '_loc.csv')


# retweet_dir = '/home/vca_rishik/rishik/COVID-19-tweets/data_retweets/'
# target_dir = retweet_dir + 'locations/'

# filename = (date.today() - timedelta(days = 4)).strftime("%m-%d-%Y")

# print("Reading file: " + filename + '.csv ... .  .')
# df = pd.read_csv(retweet_dir + filename + '.csv')
# print('done!')

# print()

# print("Extracting locations ... ")
# df['country'] = df[['entities', 'user', 'place', 'coordinates']].apply(getCountry, axis = 1)
# df[['id', 'country']].to_csv(target_dir + filename + '_loc.csv', index = None)
# print("done! saved to " + target_dir + filename + '_loc.csv')



