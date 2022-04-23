import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook


def fetch_and_process_glucose_data():
    """
    1.makes a connection to the S3 bucket 'd1namo'
    2.Fetches patient's glucose measurement data from the corresponding file URI
    3.creates join-on column to facilitate merging with other sources
    4.creates a binary column 'hypoglycemia' with the value 1 for low blood glucose levels
    and 0 otherwise, for machine learning purposes
    5.stores data as a csv file in a predefined variable in airflow's environment
    """

    import pandas as pd

    s3 = S3Hook('s3_conn').get_conn()
    if not s3:
        raise Exception('credentials are expired')

    obj = s3.get_object(
        Bucket='d1namo',
        Key='diabetes_subset_pictures-glucose-food-insulin/002/glucose.csv'
    )
    df = pd.read_csv(obj['Body'])
    df.index = pd.to_datetime(df['time'])
    df['join'] = df.index.strftime('%H:%M')
    df['hypoglycemia'] = [0 if x > 4 else 1 for x in df['glucose']]
    df.to_csv(Variable.get('glucose_data_location'))


def push_to_RDS():
    """
    1.Creates a connection to the Postgres database instance
    2.fetches the transformed data from the predefined variable in airflow's environment and stores it a dataframe
    3.pushes the data into the database
    """

    import pandas as pd

    postgres_hook = PostgresHook('postgres_conn')
    engine = postgres_hook.get_sqlalchemy_engine()

    if not engine:
        raise Exception('could not establish connection')

    my_variable = Variable.get('glucose_data_location')
    df = pd.read_csv(my_variable)
    df.to_sql(con=engine, schema='d1namo', name="patient2_glucose_data_processed_with_class",
              if_exists="append", index=False)


dag = DAG(
    'patient2_glucose_data_ETL',
    schedule_interval='@once',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1),
    render_template_as_native_obj=True)

fetch_and_process_glucose_data_task = PythonOperator(
    task_id="fetch_and_process_glucose_data",
    python_callable=fetch_and_process_glucose_data,
    dag=dag)

push_to_RDS_task = PythonOperator(
    task_id="push_to_RDS",
    python_callable=push_to_RDS,
    dag=dag)

fetch_and_process_glucose_data_task >> push_to_RDS_task

