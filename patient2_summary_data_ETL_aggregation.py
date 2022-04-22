import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

def fetch_sensor_data():
    """
    1.makes a connection to the S3 bucket 'd1namo'
    2.Fetches patient's sensor data from the corresponding file URI
    """
    import pandas as pd
    # Connect to S3
    s3 = S3Hook('s3_conn').get_conn()
    if not s3:
        raise Exception('credentials have expired')
    obj = s3.get_object(Bucket='d1namo', Key='diabetes_subset_sensor_data/002/sensor_data/2014_10_04-17_43_12/2014_10_04-17_43_12_Summary.csv')
    df = pd.read_csv(obj['Body'])
    df.to_csv(Variable.get('summary_data_location'))

def aggregate_sensor_data():
    """
    Aggregates the data by
    calculating the median for every five minutes worth of data.

    Creates a column 'join' that contains hour:minute timestamp to facilitate merging with
    other data sources.

    Returns 5-minute aggregated data with additional 'join' column
    """
    import pandas as pd

    my_variable = Variable.get('summary_data_location')
    df = pd.read_csv(my_variable)
    df.index = pd.to_datetime(df['Time'])
    df_aggregated = df.resample('5T', offset='2T').median()
    df_aggregated['join'] = df_aggregated.index.strftime('%H:%M')
    df_aggregated['date'] = df_aggregated.index.strftime('%d-%m-%Y')
    df_aggregated.to_csv(Variable.get('summary_agg_location'))

def push_to_RDS():
    """
    1.Creates a connection to the Postgres database instance
    2.fetches the aggregated data from the corresponding variable
    3.pushes the data to the database
    """
    import pandas as pd

    postgres_hook = PostgresHook('postgres_conn')
    engine = postgres_hook.get_sqlalchemy_engine()

    if not engine:
        raise Exception('could not establish connection')

    my_variable = Variable.get('summary_agg_location')
    df = pd.read_csv(my_variable)
    df.to_sql(con=engine, schema='d1namo', name="patient2_summary_data_aggregated", if_exists="append", index=False)

dag = DAG(
    'patient2_sensor_data_ETL_aggregation',
    schedule_interval='@once',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1),
    render_template_as_native_obj=True)


fetch_sensor_data_task = PythonOperator(
    task_id="fetch_sensor_data",
    python_callable=fetch_sensor_data,
    dag=dag)

aggregate_sensor_data_task = PythonOperator(
    task_id="aggregate_sensor_data",
    python_callable=aggregate_sensor_data,
    dag=dag)

push_to_RDS_task = PythonOperator(
    task_id="push_to_RDS",
    python_callable=push_to_RDS,
    dag=dag)

fetch_sensor_data_task >> aggregate_sensor_data_task >> push_to_RDS_task


