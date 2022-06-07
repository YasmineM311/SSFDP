"""
Purpose: performs a monthly call to the Clinical Trials API and pushes the data to the data lake
"""

import os
import requests
import pandas as pd
from sqlalchemy import *


engine = create_engine("postgresql://{0}:{1}@{2}:{3}/{4}".format(os.environ['user'], os.environ['password'],
                                                                  os.environ['host'], os.environ['port'],
                                                                  os.environ['database']))

## API URL path with required fields specified
URL ='https://clinicaltrials.gov/api/query/study_fields?expr=diabetes&fields=Condition,StartDate,BriefTitle,LocationCountry,StudyType,OverallStatus,HealthyVolunteers,PointOfContactEMail&max_rnk=500&fmt=json'


def lambda_handler(event, context):
    # Push data to data lake
    r = requests.get(URL)
    content = r.json()['StudyFieldsResponse']['StudyFields']
    df = pd.json_normalize(content)
    df.to_sql(con=engine, schema='ClinicalTrialsAPI', name ="ClinicalTrials", if_exists="replace", index=False)
