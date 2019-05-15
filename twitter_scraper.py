############################## Load Modules #################################

# twitter api
import tweepy
from tweepy.streaming import StreamListener

# json
import json

# Google Cloud
from google.cloud import language, storage

# Formatting
from pprint import pprint

# Datetime Manipulation
import datetime
import pytz
from pytz import timezone
from apscheduler.schedulers.blocking import BlockingScheduler

# Linear Algebra
import pandas as pd
import numpy as np
from IPython.display import display

# Visulizations
import matplotlib.pyplot as plt

############################## Authorizations #################################

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "data/My First Project-f196f491b645.json"

def get_tweets():
    
    twitter_auth = json.load(open('data/twitter_auth.json', 'r'))
        
    CONSUMER_KEY = twitter_auth['consumer_key']
    CONSUMER_SECRET = twitter_auth['consumer_secret']
    ACCESS_TOKEN = twitter_auth['access_token']
    ACCESS_TOKEN_SECRET = twitter_auth['access_token_secret']

    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    api = tweepy.API(auth)
    public_tweets = tweepy.Cursor(api.search, q='Louis C.K.', tweet_mode='extended').items()
    #stream = tweepy.Stream()
    #stream.filter(track=['Louis C.K.'])
    #public_tweets = 

    return public_tweets

############################## Helper Funcs ################################

def language_analysis(text):
    client = language.LanguageServiceClient()
    document = language.types.Document(
        content=text,
        type=language.enums.Document.Type.PLAIN_TEXT
    )
    sent_analysis = client.analyze_sentiment(document)
    sentiment = sent_analysis.document_sentiment
    return sentiment


def get_tweet_dataframe():
    # Initialize an empty dataframe
    tweet_df = pd.DataFrame(
        columns = [
        'user', 
        'full_text', 
        'location', 
        'time_stamp', 
        'sentiment_score', 
        'sentiment_magnitude']
    )

    # Get tweets
    public_tweets = get_tweets()

    count = 0
    err_count = 0
    sentiments = []
    delta = datetime.timedelta(hours=5)


    for tweet in public_tweets:

        if not tweet.user.location or tweet.lang!='en':
            err_count += 1
            if err_count%100==0:
                print err_count
            continue

        if count>0 and count%10==0:
            break

        utc_dt = tweet.created_at
        loc_dt = utc_dt - delta
        full_tweet = ""
        if tweet.full_text.startswith('RT'):
            try:
                full_tweet = tweet.retweeted_status.full_text
            except:
                full_tweet = tweet_full_text
        else:
            full_tweet = tweet.full_text
        sentiment = language_analysis(full_tweet)

        cur_tweet_df = pd.DataFrame({
            'user': [tweet.user.screen_name],
            'full_text': [full_tweet],
            'location': [tweet.user.location],
            'time_stamp': [loc_dt],
            'sentiment_score': [sentiment.score],
            'sentiment_magnitude': [sentiment.magnitude]
        }
        )
        tweet_df = tweet_df.append(cur_tweet_df, ignore_index=True)
        count += 1

    return tweet_df
    
def create_csv():
    tweet_df = get_tweet_dataframe()
    now = datetime.datetime.today()
    month = now.month
    day = now.day
    hour = now.hour
    file_path = 'data/tweets_{0}_{1}_{2}.csv'.format(month, day, hour)
    tweet_df.to_csv(file_path, index=False, encoding='utf-8')

############################## Main Func #################################

if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(create_csv, 'interval', hours=1)
    scheduler.start()  
    #create_csv()


























