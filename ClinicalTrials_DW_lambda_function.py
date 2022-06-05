"""
Purpose:
1. Establishes connections to the data lake and data warehouse postgres instances
2. Queries the data lake's schema (ClinicalTrialsAPI) that has full information
about diabetes clinical trials retrieved through the clinical trials
API
3. Creates a tables with only recent trials (last 4 years) in the data warehouse

The lambda function is triggered once a month
"""
import os
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

## Creating connections to data lake and data warehouse
engine1 = create_engine("postgresql://{0}:{1}@{2}:{3}/{4}".format(os.environ['user'], os.environ['password'],
                                                                  os.environ['host1'], os.environ['port'],
                                                                  os.environ['database']))
engine2 = create_engine("postgresql://{0}:{1}@{2}:{3}/{4}".format(os.environ['user'], os.environ['password'],
                                                                  os.environ['host2'], os.environ['port'],
                                                                  os.environ['database']))

## selecting trials in the last 4 years using a string search
sql_query = """SELECT *
              From "ClinicalTrialsAPI"."ClinicalTrials"
              WHERE "StartDate"  ~* '2018' OR
              "StartDate"  ~* '2019' OR
              "StartDate"  ~* '2020' OR
              "StartDate"  ~* '2021' OR
              "StartDate"  ~* '2022';"""


def lambda_handler(event, context):
    # executing the sql query and pushing the results to the data warehouse
    result = engine1.execute(sql_query)
    df = pd.DataFrame(result, columns=result.keys())
    df.to_sql(con=engine2, schema='ClinicalTrialsDW', name="recent_studies", if_exists="replace", index=False)