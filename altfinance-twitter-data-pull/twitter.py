import tweepy
import os
from dotenv import load_dotenv
import pandas as pd
import datetime as dt
from datetime import timedelta

load_dotenv()

# importing twitter api credentials 
consumer_key = os.environ["consumer_key"]
consumer_secret = os.environ["consumer_secret"]
access_token = os.environ["access_token"]
access_token_secret = os.environ["access_token_secret"]

auth = tweepy.OAuth1UserHandler(
  consumer_key, 
  consumer_secret, 
  access_token, 
  access_token_secret
)

api = tweepy.API(auth)


def twitter_search(query):
    df = pd.DataFrame(columns='id text created_at user_screen_name user_followers_count Retweets Likes'.split())


    length = 100
    today = dt.datetime.utcnow() + timedelta(days=1)
    new_until = str(today)

    while length == 100:
        new_until = new_until[:new_until.index(" ")]
        alt_tweets = api.search_tweets(query, tweet_mode="extended",lang = 'en',count=101,until = new_until)
        
        data = {
        "id": [tweet.id_str for tweet in alt_tweets],
        "text": [tweet.full_text for tweet in alt_tweets],
        "created_at": [tweet.created_at for tweet in alt_tweets],
        "user_screen_name": [tweet.user.screen_name for tweet in alt_tweets],
        "user_followers_count": [tweet.user.followers_count for tweet in alt_tweets],
        'Retweets': [tweet.retweet_count for tweet in alt_tweets],'Likes': [tweet.favorite_count for tweet in alt_tweets]}
        
        df1 = pd.DataFrame(data)
        
        df = pd.concat([df,df1])
        
        length = len(df1)
        
        new_until = df1.created_at.min() + timedelta(hours=24)
        
        new_until = str(new_until)
    
    convert_dict = {'id': str,'user_followers_count':int
            }
  
    df = df.astype(convert_dict)

    df['id'] = 'tw' + df.id
    
    return df 