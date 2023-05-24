import datetime as dt
import logging
import os
import pandas as pd
import numpy as np

from datetime import timedelta
from dotenv import load_dotenv
from . import azure_blob_uploader,sentiment_models,twitter,mail_sender,google_app_store_pipeline

import azure.functions as func

load_dotenv()

accountName = os.environ['ACCOUNT_NAME']
containerName = os.environ['containerName']

def code_runner():
# altfinance
    df = twitter.twitter_search('altfinanceng')

#altbank
    df1 = twitter.twitter_search('altbank')

    df = pd.concat([df,df1])

#altmall
    df1 = twitter.twitter_search('altmall')

    df = pd.concat([df,df1])

#altinvest
    df1 = twitter.twitter_search('altinvest')

    df = pd.concat([df,df1])

# consolidate and transform
    df = df.drop_duplicates()

    alt_screen_names = 'AltFinanceNg AltInvestng Altmallng'.split()

    df = df[~df.user_screen_name.isin(alt_screen_names)].copy()

    # removing retweeted tweets
    df['is_rt'] = df.text.apply(lambda x:x[:2])
    df = df[df.is_rt != 'RT']

    df.drop('is_rt',axis=1,inplace=True)

    df2 = pd.read_csv(f"https://{accountName}.blob.core.windows.net/{containerName}/saf_twitter_sentiment.csv")

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
    
    google_app_store_pipeline.app_reviews()
    
    mail_sender.send_email(send_to=recepient,subject=subject,body=message)


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = dt.datetime.utcnow().replace(
        tzinfo=dt.timezone.utc).isoformat()

    code_runner()

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
