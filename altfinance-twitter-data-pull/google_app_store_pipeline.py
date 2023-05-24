from app_store_scraper import AppStore
from google_play_scraper import app,Sort, reviews_all
from . import azure_blob_uploader,sentiment_models

import pandas as pd
import numpy as np
import json


def app_reviews():
    app_dict = {
        'altbank':'1513246800',
        'altpro':'1559357720',
        'altinvest':'1526012590',
        'activ8it':'1622615046'
    }

    def appstore_reviews(name,id):
        rev = AppStore(country='ng', app_name=name, app_id = id)
        rev_app = rev.review()
        df = pd.DataFrame(rev.reviews)
        df['product'] = name
        return df

    headers = ['product', 'date', 'review', 'rating', 'isEdited', 'title', 'userName',
        'developerResponse']

    df = pd.DataFrame(columns=headers)

    for x in app_dict:
        df1 = appstore_reviews(x,app_dict[x])
        df = pd.concat([df,df1])

    df['Sentiment'] = df.review.apply(sentiment_models.distilbert_model)

    df['Sentiment2'] = df.review.apply(sentiment_models.robertabase_model)

    df['SENTIMENT'] = df.apply(lambda x: sentiment_models.sentiment_model(x.Sentiment, x.Sentiment2), axis=1)

    df = df.drop('Sentiment Sentiment2'.split(),axis=1)

    df['tweet_corpus'] = df.review.apply(sentiment_models.text_process)

    azure_blob_uploader.blob_uploader(data=df,file_name='app_store_reviews.csv')

    def playstore_reviews(app_id):
        reviews = reviews_all(app_id)
        df_reviews = pd.DataFrame(np.array(reviews),columns=['review'])
        df_reviews = df_reviews.join(pd.DataFrame(df_reviews.pop('review').to_list()))

        arr = [letter[0] for letter in enumerate(app_id) if letter[1] == '.']
        arr.sort()
        pos = arr[len(arr)-1]
        product_name = app_id[pos+1:]

        df_reviews['product'] = product_name
        return df_reviews

    headers = ['reviewId', 'userName', 'userImage', 'content', 'score',
        'thumbsUpCount', 'reviewCreatedVersion', 'at', 'replyContent',
        'repliedAt', 'appVersion', 'product']


    apps = 'ng.sterling.altbank ng.sterling.altpro ng.saf.activ8 ng.sterling.AltInvest'.split()

    df = pd.DataFrame(columns=headers)
    for app in apps:
        df1 = playstore_reviews(app)
        df = pd.concat([df,df1])


    df['Sentiment'] = df.content.apply(sentiment_models.distilbert_model)

    df['Sentiment2'] = df.content.apply(sentiment_models.robertabase_model)

    df['SENTIMENT'] = df.apply(lambda x: sentiment_models.sentiment_model(x.Sentiment, x.Sentiment2), axis=1)

    df = df.drop('Sentiment Sentiment2'.split(),axis=1)

    df['tweet_corpus'] = df.content.apply(sentiment_models.text_process)

    azure_blob_uploader.blob_uploader(data=df,file_name='playstore_reviews.csv')

    print ('all files uploaded')

