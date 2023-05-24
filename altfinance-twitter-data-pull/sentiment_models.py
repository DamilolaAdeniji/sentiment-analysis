from transformers import pipeline
import string
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords


sentiment_pipeline = pipeline("sentiment-analysis")
sentiment_pipeline2 = pipeline("sentiment-analysis",model='cardiffnlp/twitter-roberta-base-sentiment')

def distilbert_model(text):
    label = sentiment_pipeline2(text)[0]['label']
    
    if label == 'LABEL_2':
        return 'Positive'
    elif label == 'LABEL_1':
        return 'Neutral'
    else: return 'Negative'


def robertabase_model(text):
    return sentiment_pipeline(text)[0]['label']


def sentiment_model(output1,output2):
    """
    output1 & output2
    Positive & POSITIVE -- good
    Positive & NEGATIVE -- bad

    Negative & NEGATIVE -- bad
    Negative & POSITIVE -- bad

    Neutral & NEGATIVE -- bad
    Neutral & POSITIVE -- Neutral
    """
    if output1 == 'Positive' and output2 == 'POSITIVE':
        return 'Positive'
    elif output1 == 'Neutral' and output2 == 'POSITIVE':
        return 'Neutral'
    else:
        return 'Negative'

new_stopwords = stopwords.words('english')

add_words = "pls !!!!!! go last still get nothing Hello it's Im Na sssit without also make dey getting done really don't act try person always ive use one open I've".split()

for word in add_words:
    new_stopwords.append(word)
new_stopwords = [word.lower() for word in new_stopwords]


def text_process(text):
    """
    Takes in a string of text, then performs the following:
    1. Remove all punctuation
    2. Remove all stopwords
    3. Returns a list of the cleaned text
    """
    # remove mentions/twitter handles
    text = ' '.join([word for word in text.split() if '@' not in word])
    
    # Check characters to see if they are in punctuation
    nopunc = [char for char in text if char not in string.punctuation]

    # Join the characters again to form the string.
    nopunc = ''.join(nopunc)
    
    # Now just remove any stopwords
    return ' '.join([word for word in nopunc.split() if word.lower() not in new_stopwords])