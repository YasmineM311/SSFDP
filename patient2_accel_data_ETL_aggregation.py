import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook


def fetch_accel_data():
    """
    1.makes a connection to the S3 bucket 'd1namo'
    2.Fetches patient's accelerometer data from the corresponding file URI
    3.stores data as a csv file in a predefined variable in airflow's environment
    """

    import pandas as pd
    # Connect to S3
    s3 = S3Hook('s3_conn').get_conn()
    if not s3:
        raise Exception('credentials have expired')
    obj = s3.get_object(
        Bucket='d1namo',
        Key='diabetes_subset_sensor_data/002/sensor_data/2014_10_02-06_44_21/2014_10_02-06_44_21_Accel.csv'
    )
    df_initial = pd.read_csv(obj['Body'])
    df = df_initial.iloc[3000001:3500000]
    df.to_csv(Variable.get('accel_data_location'))


def aggregate_accel_data():
    """
    1.fetches csv file from the predefined variable in airflow's environment and stores it as a dataframe
    2.aggregates the data by calculating the median for every five minutes worth of data
    3.stores aggregated data as a csv file in a predefined variable in airflow's environment
    """

    import pandas as pd

    my_variable = Variable.get('accel_data_location')
    df = pd.read_csv(my_variable)
    df.index = pd.to_datetime(df['Time'])
    df_aggregated = df.resample('5T', offset='2T').median()
    df_aggregated.to_csv(Variable.get('accel_agg_location'))


def create_join_columns():
    """
    1.fetches csv file from the predefined variable in airflow's environment and stores it as a dataframe
    2.creates join-on columns to facilitate merging with other data sources
    3.stores transformed data as a csv file in a predefined variable in airflow's environment
    """

    import pandas as pd
    my_variable = Variable.get('accel_agg_location')
    df_aggregated = pd.read_csv(my_variable)
    df_aggregated.index = pd.to_datetime(df_aggregated['Time'])
    df_aggregated['join'] = df_aggregated.index.strftime('%H:%M')
    df_aggregated['date'] = df_aggregated.index.strftime('%d-%m-%Y')
    df_aggregated.to_csv(Variable.get('accel_agg_location'))


def push_to_RDS():
    """
    1.Creates a connection to the Postgres database instance
    2.fetches the aggregated data from the predefined variable in airflow's environment and stores it a dataframe
    3.pushes the data into the database
    """

    import pandas as pd

    postgres_hook = PostgresHook('postgres_conn')
    engine = postgres_hook.get_sqlalchemy_engine()
    if not engine:
        raise Exception('could not establish connection')

    my_variable = Variable.get('accel_agg_location')
    df = pd.read_csv(my_variable)
    df.to_sql(con=engine, schema='d1namo', name="patient2_accel_data_aggregated", if_exists="append", index=False)


dag = DAG(
    'patient2_accel_data_ETL_aggregation',
    schedule_interval='@once',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1),
    render_template_as_native_obj=True)


fetch_accel_data_task = PythonOperator(
    task_id="fetch_accel_data",
    python_callable=fetch_accel_data,
    dag=dag)

aggregate_accel_data_task = PythonOperator(
    task_id="aggregate_accel_data",
    python_callable=aggregate_accel_data,
    dag=dag)

create_join_columns_task = PythonOperator(
    task_id='create_join_columns',
    python_callable=create_join_columns,
    dag=dag
)

push_to_RDS_task = PythonOperator(
    task_id="push_to_RDS",
    python_callable=push_to_RDS,
    dag=dag)

fetch_accel_data_task >> aggregate_accel_data_task >> create_join_columns_task >> push_to_RDS_task

