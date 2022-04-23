# SSFDP
This a project to create a data lake and data warehouse for a hypothetical high tech healthcare company that aims at developing new technologies to improve the quality of life of diabetic patients

# Sources
## Springer Nature API
#### SpringerAPI_lambda_function1.py 

Description & goal: Lambda function triggered weekly to query the Springer API, with 'diabetes AND wearables' as a search keyword. It aims to retreive the most recent publications about using wearable technology in diabetes management.

Author: Yasmine Mohamed

Remarks: For the lambda function to work, the postgres database credentials have to be added to the lambda function's environment. Additionally, cloud 9 layer with the required packages (pandas, requests, psycopg2, sqlalchemy) had to be added to the lambda function.

#### SpringerAPI_lambda_function2.py

Description & goal: Lambda function triggered weekly to query the Springer API, with 'diabetes' as a search keyword. It aims to retreive the most recent publications about diabetes in general. The data is then pushed into a postgres instance

Author: Yasmine Mohamed

Remarks: For the lambda function to work, the postgres database credentials have to be added to the lambda function's environment. Additionally, cloud 9 layer with the required packages (pandas, requests, psycopg2, sqlalchemy) had to be added to the lambda function.

## D1namo 
#### migrating the d1namo dataset to an s3 bucket.ipynb

Description & goal: To migrate the d1namo dataset into an s3 bucket to be further processes using Apache Airflow ETL pipelines

Remarks: The AWS credentials have to be added, in order for the connection to the s3 bucket to be established

Author: Yasmine Mohamed

#### patient2_accel_data_ETL_aggregation.py

Description & goal: A DAG to process raw sensor data from an accelerometer and then push the processed data into a postgres instance


- DAG 2, for summarized sensor data: https://github.com/YasmineM311/SSFDP/blob/main/patient2_summary_data_ETL_aggregation.py
- DAG 3, for continuous glucose monitoring data: https://github.com/YasmineM311/SSFDP/blob/main/patient2_glucose_data_ETL.py 

## Clinical Trials


#### docker-compose.yaml

Description & goal: Configuration file for the initiliastion of the Apache Airflow services in the Docker container
Remarks: 'docker-compose airflow init' has to be run the first time of execution to create the Docker container. 

Author: Philipp Eble

#### client.py

Description & goal: Class definition for the API requests for the API URL (Full Studies or Study Fields)
Remarks: For using related DAGs you have to use the 'CSV' format in the query. For other purposes a JSON format can also be selected. 

Author: Philipp Eble


#### utils.py

Description & goal: Definition of the functions for handling HTTP requests and decoding it into JSON or CSV format
Remarks: None 

Author: Philipp Eble


#### results_dag.py 

Description & goal: DAG for running API calls, temporarily storaging output in a dataframe, establishing the connection and finally dumping data into the database
Remarks: The name of the postgres database must be adjusted and must be a saved connection in the Apachae Airflow webserver to ensure authorization.  

Author: Philipp Eble

 
## Static data

#### Static_data.ipynb
Description & goal: Migrate data form the API to a single S3 AWS folder.

Remarks: The AWS credentials have to be added, in order for the connection to the s3 bucket to be established

Author: Carla Frege

