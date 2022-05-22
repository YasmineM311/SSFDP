# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import boto3
import botocore
#import data from s3 bucket

BUCKET_NAME = 's3diabetes' 
KEY = 'diabetes-data/s3diabeteskaggle.csv' # replace with your object key

s3 = boto3.resource('s3')

try:
    # download as local file
    s3.Bucket(BUCKET_NAME).download_file(KEY, 'diabetes-data/s3diabeteskaggle.csv')

    # OR read directly to memory as bytes:
    # bytes = s3.Object(BUCKET_NAME, KEY).get()['Body'].read() 
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise
#read the data
df_kaggle = pd.read_csv('diabetes-data/s3diabeteskaggle.csv')
df_kaggle.head()

#Prepare the data for ML

train, test = train_test_split(df_kaggle, test_size=0.65) #65% beacuse SageMaker needs at least 250 entries
