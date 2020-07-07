import re
import pandas as pd
import numpy as np
import pickle
from flask import Flask
from flask import request
import json
import psycopg2
import requests
import sklearn
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


requests.get('http://127.0.0.1:3000/get_data_count', params={'label_name': 'positive','count': 1000 })
requests.get('http://127.0.0.1:3000/get_data_count', params={'label_name': 'negative','count': 0 })


from database_service import get_data

URL2 = "http://127.0.0.1:3000/get_data"
param = {'count': 1000, 'sort_order': 'ASC'}
response_json = requests.get(url=URL2, params=param, headers={'Content-Type': 'application/json'})
#print(response_json.json())

test_data = response_json.json()
print ("total number of entries imported from labels DB = ", len(test_data))

#defining clean text function

def clean_text(text):
    
    text = text.lower()
    text = re.sub("@[a-z0-9_]+", ' ', text)
    text = re.sub("[^ ]+\.[^ ]+", ' ', text)
    text = re.sub("[^ ]+@[^ ]+\.[^ ]", ' ', text)
    text = re.sub("[^a-z\' ]", ' ', text)
    text = re.sub(' +', ' ', text)

    return text

#Creating clean lists of test texts and test labels
X_text=[]
X_labels=[]
positive_count = 0
negative_count = 0
for i in range(len(test_data)):
    X_text.append(clean_text(test_data[i][0]))
    X_labels.append(test_data[i][1])
    if test_data[i][1] == 0:
        negative_count+=1
    else:
        positive_count+=1

print('Total number of positive texts = ', positive_count)
print('Total number of negative texts = ', negative_count)

#importing and converting model.pickle and vectorizor.pickle
with open('model.pickle', 'rb') as file:
model = pickle.load(file)

with open('vectorizer.pickle', 'rb') as file:
vectorizer = pickle.load(file)

#vectorizing test data

from sklearn.feature_extraction.text import CountVectorizer

#vectorizer = CountVectorizer(analyzer='word', token_pattern=r'\w{1,}', min_df=1)

X_text = vectorizer.transform(X_text)
#X_text = vectorizer.transform(X_text)

#measuring accuracy of model

def measure_accuracy (X_text, X_labels):
    from sklearn.metrics import accuracy_score

    predictions = model.predict(X_text)

    model_accuracy = accuracy_score(X_labels, predictions)

    print("accuracy score using naive_bayes: ")
    print(model_accuracy)

    return model_accuracy

print (measure_accuracy(X_text, X_labels))



