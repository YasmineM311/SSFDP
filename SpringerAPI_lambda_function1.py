"""
Purpose:
1. Queries Springer's API (a publisher of scientific research) using the keyword 'diabetes AND wearables'
2. Extracts relevant information from the json data into a pandas data frame
3. Establishes a connection to database instance "datalakeym1"
4. Pushes retrieved data to the database instance

The lambda function is triggered once weekly
"""

import os
import requests
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2

url = "http://api.springernature.com/metadata/json?p=100&q=keyword:diabetes AND wearables&api_key=d8a0b26cff2607963363c0256b43d3d9"
data_dict = {'date': [], 'url': [], 'title': [], 'abstract': [], 'journal': []}


def get_connection():
    """
    returns a connection to the database instance
    """

    return create_engine(
        "postgresql://{0}:{1}@{2}:{3}/{4}".format(os.environ['user'], os.environ['password'], os.environ['host'],
                                                  os.environ['port'], os.environ['database']))


def json_to_df():
    """
    1. Does the API call and stores the publication records in the variable 'records'
    2. Extracts the relevant information from each record and stores them in a dict which is then
       transformed into a pandas dataframe
    3. Returns the dataframe
    """

    r = requests.get(url)
    records = r.json()['records']

    for record in records:
        data = {'date': record['publicationDate'],
                'title': record['title'],
                'abstract': record['abstract'],
                'journal': record['publicationName'],
                'url': record['url'][0]['value']}
        for x in data.keys():
            for y in data_dict.keys():
                if x == y:
                    data_dict[y].append(data[x])

    df = pd.DataFrame.from_dict(data_dict)
    return df


def lambda_handler(event, context):
    """
    Upon being triggered, this lambda handler:
    1.creates a connection engine and stores it in the variable 'engine'
    2.queries the API and stores the retrieved data in a dataframe
    3.pushes the data into the database instance
    """

    engine = get_connection()
    df = json_to_df()
    df.to_sql(
        con=engine, schema='SpringerPublications', name="DiabetesAndWearables",
        if_exists="replace", index=False,
        dtype={'date': sqlalchemy.types.Date(), 'title': sqlalchemy.types.Text(),
               'abstract': sqlalchemy.types.Text(), 'journal': sqlalchemy.types.Text(),
               'url': sqlalchemy.types.Text()}
    )


