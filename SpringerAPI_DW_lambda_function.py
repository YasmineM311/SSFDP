"""
Purpose:
1. Establishes connections to the data lake and data warehouse postgres instances
2. Queries the data lake's schema (diabetesandwearables) that has full information
about most recent publications retrieved through the Springer Nature API using
the search words "diabetes" and "wearables"
3. Creates three tables in the data warehouse that provide useful insights

The lambda function is triggered once weekly
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

## SQL query to retrieve weekly number of publications
sql_query_1 = """SELECT date_trunc('week', date::date) AS weekly,
                COUNT(*)
                FROM "SpringerPublications"."diabetesandwearables"
                GROUP BY weekly
                order by weekly;"""

# SQL query to retrieve the titles and URLS of publications published in the past month
sql_query_2 = """SELECT title, URL 
                FROM "SpringerPublications"."diabetesandwearables" 
                WHERE date BETWEEN (current_date - interval '30 days') AND current_date;"""

## SQL queries that retrieve the number of times certain words of interest have been mentioned in the past 7 days
## list of words of interest
words = ["'type 1'", "'type 2'", "'cgm'", "'IoT'", "'Machine Learning'", "'wearable'", "'non-invasive'"]

## creating a list of SQL queries, one for each word
word_query_string = []

for word in words:
    query = """SELECT {0}, COUNT(abstract) + COUNT(title) AS {1}
               FROM "SpringerPublications"."diabetesandwearables" 
               WHERE title ~* {0} OR abstract ~* {0}
               AND date BETWEEN (current_date - interval '7 days') AND current_date;""".format(word, word[1:-1].replace(" ", "").replace("-", "_"))
    if query is not None:
        word_query_string.append(query)


def lambda_handler(event, context):
    # executing first SQL query and pushing the results to the data warehouse
    result1 = engine1.execute(sql_query_1)
    df = pd.DataFrame(result1, columns=["week", "count"])
    df.to_sql(con=engine2, schema='SpringerPublicationsDW', name="weekly_stats", if_exists="replace", index=False)

    # executing second SQL query and pushing the results to the data warehouse
    result2 = engine1.execute(sql_query_2)
    df2 = pd.DataFrame(result2, columns=["title", "URL"])
    df2.to_sql(con=engine2, schema='SpringerPublicationsDW', name="title_url", if_exists="replace", index=False)

    # executing sequence of queries to retrieve words of interest count and pushing the results to the data warehouse
    data = []

    for query in word_query_string:
        result = engine1.execute(query).first()
        data.append(result)
    df = pd.DataFrame.from_records(data, columns=['word', 'count'])
    df.to_sql(con=engine2, schema='SpringerPublicationsDW', name="words_in_focus", if_exists="replace", index=False)
