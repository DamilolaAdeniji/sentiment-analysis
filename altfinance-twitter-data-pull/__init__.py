import datetime as dt
import logging
import os
import pandas as pd
import numpy as np

from datetime import timedelta
from dotenv import load_dotenv
from . import azure_blob_uploader,sentiment_models,twitter,mail_sender

import azure.functions as func

load_dotenv()

def code_runner():
    api = twitter.api

    df = pd.DataFrame(columns='id text created_at user_screen_name user_followers_count Retweets Likes'.split())

    length = 100
    today = dt.datetime.utcnow() + timedelta(days=1)
    new_until = str(today)



    while length == 100:
        new_until = new_until[:new_until.index(" ")]
        alt_tweets = api.search_tweets("altfinanceng", tweet_mode="extended",lang = 'en',count=101,until = new_until)
        
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
    
    length = 100
    today = dt.datetime.utcnow()
    new_until = str(today)

    dfb = pd.DataFrame(columns='id text created_at user_screen_name user_followers_count Retweets Likes'.split())
    while length == 100:
        new_until = new_until[:new_until.index(" ")]
        alt_tweets = api.search_tweets("altbank", tweet_mode="extended",lang = 'en',count=101,until = new_until)
        
        data = {
        "id": [tweet.id_str for tweet in alt_tweets],
        "text": [tweet.full_text for tweet in alt_tweets],
        "created_at": [tweet.created_at for tweet in alt_tweets],
        "user_screen_name": [tweet.user.screen_name for tweet in alt_tweets],
        "user_followers_count": [tweet.user.followers_count for tweet in alt_tweets],
        'Retweets': [tweet.retweet_count for tweet in alt_tweets],'Likes': [tweet.favorite_count for tweet in alt_tweets]}
        
        df1 = pd.DataFrame(data)
        
        dfb = pd.concat([dfb,df1])
        
        length = len(df1)
        
        new_until = df1.created_at.min() + timedelta(hours=24)
        
        new_until = str(new_until)    








    df = pd.concat([dfb,df])
        
    df = df.drop_duplicates()

    df = df[df.user_screen_name != 'AltFinanceNg'].copy()

    # removing retweeted tweets
    df['is_rt'] = df.text.apply(lambda x:x[:2])
    df = df[df.is_rt != 'RT']

    df.drop('is_rt',axis=1,inplace=True)

    df2 = pd.read_csv('https://safdatateamstorage.blob.core.windows.net/netcoreblob/saf_twitter_sentiment.csv')

    convert_dict = {'id': str,'user_followers_count':int
            }

    df2 = df2.astype(convert_dict)

    df = df.astype(convert_dict)

    df = df[~df.id.isin(df2.id)]

    if len(df) > 0:

        df['Sentiment'] = df.text.apply(sentiment_models.distilbert_model)

        df['Sentiment2'] = df.text.apply(sentiment_models.robertabase_model)

        df['SENTIMENT'] = df.apply(lambda x: sentiment_models.sentiment_model(x.Sentiment, x.Sentiment2), axis=1)

        df = df.drop('Sentiment Sentiment2'.split(),axis=1)

        df['tweet_corpus'] = df.text.apply(sentiment_models.text_process)

        df = pd.concat([df,df2])

        df = df.drop_duplicates()

        azure_blob_uploader.blob_uploader(data=df,file_name='saf_twitter_sentiment.csv')

        subject = 'TWITTER DATA REFRESHED'

        message = """
            <html>
            <head>
            </head>
            <body>
            """ + 'Hello' + """ Dammy,<br>
                                    <p> Twitter data has been loaded to Blob!.""" """<br>
            </p>Warm Regards.<br>
                                </p>SAF Analytics
                                </body>
                                """
    else:
        subject = 'NO TWITTER DATA TO REFRESH'
        message = """
            <html>
            <head>
            </head>
            <body>
            """ + 'Hello' + """ Dammy,<br>
                                    <p> No new tweets to be uploaded!.""" """<br>
            </p>Warm Regards.<br>
                                </p>SAF Analytics
                                </body>
                                """



    recepient = os.environ['Dammy']
    

    mail_sender.send_email(send_to=recepient,subject=subject,body=message)


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = dt.datetime.utcnow().replace(
        tzinfo=dt.timezone.utc).isoformat()

    code_runner()

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
