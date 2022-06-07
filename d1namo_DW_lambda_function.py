"""
Purpose:
1. establish connections to data lake postgres instance, data warehouse postgres instance and s3 bucket.
2. execute sql query that joins two tables from the data lake and push the combined data into:
  a. the data warehouse to easily visualize and validate the data
  b. the s3 bucket, to easily connect to amazon SageMaker, an AWS machine learning tool
"""

import os
import pandas as pd
from sqlalchemy import create_engine
import boto3
import psycopg2
from io import StringIO
import boto3

engine = create_engine("postgresql://{0}:{1}@{2}:{3}/{4}".format(os.environ['user'], os.environ['password'],
                                                                 os.environ['host'], os.environ['port'],
                                                                 os.environ['database']))

engine2 = create_engine("postgresql://{0}:{1}@{2}:{3}/{4}".format(os.environ['user'], os.environ['password'],
                                                                  os.environ['host2'], os.environ['port'],
                                                                  os.environ['database']))

sql_query = """ SELECT * 
                FROM "d1namo".patient2_summary_data_aggregated   
                natural JOIN "d1namo".patient2_glucose_data_processed_with_class;"""

s3_resource = boto3.resource('s3')

bucket = 'd1namo'


def lambda_handler(event, context):
    # execute sql query and store results in a data frame
    result = engine.execute(sql_query)
    df = pd.DataFrame(result, columns=result.keys())

    # load the result into s3 bucket
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource.Object(bucket, 'patient2_combined_data.csv').put(Body=csv_buffer.getvalue())

    # load the result into data warehouse
    df.to_sql(con=engine2, schema="d1namoDW", name="patient2_combined_data", if_exists="replace", index=False)

