import datetime
import logging
import pandas as pd
from client import ClinicalTrials

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator


#set-up scheduler 
DAG = DAG(
    dag_id='results_dag',
    start_date = datetime.datetime.now(),
    schedule_interval="@hourly" 
    )

#grab data from clinicaltrials API 
def api_call(**context):
    ct = ClinicalTrials()
    diabetes_fields = ct.get_study_fields(
        search_expr= "Diabetes",
        fields=[
        "NCTId", 
        "Condition", 
        "EnrollmentCount",
        "InterventionName"],
        max_studies=5,
        fmt="csv",)
    task_instance = context['task_instance']
    task_instance.xcom_push(key="diabetes_fields", value=diabetes_fields)



#read data into pandas dataframe
def read_data(**kwargs):
    ti = kwargs['ti']
    db_hook = PostgresHook(postgres_conn_id="datalake2", schema="datalake2")
    fields = ti.xcom_pull(task_ids='fetch_csv_fields', key='diabetes_fields')
    df = pd.DataFrame.from_records(fields[1:], columns=fields[0])
    rows = list(df.itertuples(index = False))
    for row in rows:
        sql = f'INSERT INTO clinical_trials (NCTId, Condition, EnrollmentCount, InterventionName) VALUES {str(row[1:])}'
        db_hook.run(sql)


fetch_csv_fields = PythonOperator(
    task_id='fetch_csv_fields',
    python_callable=api_call,
    dag=DAG)

pull_data = PythonOperator(
    task_id='pull_data',
    python_callable=read_data,
    dag=DAG)

#create table inside of AWS datalake
create_table = PostgresOperator(
    task_id  = 'create_table',
    dag = DAG, 
    postgres_conn_id = "datalake2",
    sql ='''
            CREATE TABLE IF NOT EXISTS clinical_trials (NCTId text, Condition text, EnrollmentCount text, InterventionName text);
         '''
)



fetch_csv_fields >> create_table >> pull_data
