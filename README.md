# SSFDP
This a project to create a data lake and data warehouse for a hypothetical high tech healthcare company that aims at developing new technologies to improve the quality of life of diabetic patients

# Sources
## Springer Nature API
#### SpringerAPI_lambda_function1.py 

Description & goal: Lambda function triggered weekly to query the Springer API, with 'diabetes AND wearables' as a search keyword. It aims to retreive the most recent publications about using wearable technology in diabetes management.

Remarks: For the lambda function to work, the postgres database credentials have to be added to the lambda function's environment. Additionally, cloud 9 layer with the required packages (pandas, requests, psycopg2, sqlalchemy) had to be added to the lambda function.

Author: Yasmine Mohamed

#### SpringerAPI_lambda_function2.py

Description & goal: Lambda function triggered weekly to query the Springer API, with 'diabetes' as a search keyword. It aims to retreive the most recent publications about diabetes in general. The data is then pushed into a postgres instance

Remarks: For the lambda function to work, the postgres database credentials have to be added to the lambda function's environment. Additionally, cloud 9 layer with the required packages (pandas, requests, psycopg2, sqlalchemy) had to be added to the lambda function.

Author: Yasmine Mohamed

#### SpringerAPI_DW_lambda_function.py

Description & goal: This lambda function is triggered weekly to:
1. Establish connections to the data lake and data warehouse postgres instances
2. Query the data lake's schema (diabetesandwearables) that has full information about most recent publications retrieved through the Springer Nature API using the search words "diabetes" and "wearables"
3. Create three tables in the data warehouse that provide useful insights

Remarks: For the lambda function to work, the postgres database credentials have to be added to the lambda function's environment. Additionally, cloud 9 layer with the required packages (pandas, requests, psycopg2, sqlalchemy) had to be added to the lambda function and the execution timeout has to be extended to 1 minute.

Author: Yasmine Mohamed

## D1namo 
#### migrating the d1namo dataset to an s3 bucket.ipynb

Description & goal: To migrate the d1namo dataset into an s3 bucket to be further processed using Apache Airflow ETL pipelines

Remarks: The AWS credentials have to be added, in order for the connection to the s3 bucket to be established

Author: Yasmine Mohamed

#### patient2_accel_data_ETL_aggregation.py

Description & goal: A DAG to process patient data obtained from a sensor (accelerometer) and then push the processed data into a postgres instance

Remarks: To be able to run the dag, connections to the postgres instance and s3 bucket must be added. In addition, the referenced variables must be added to Airflow's environment

Author: Yasmine Mohamed

#### patient2_summary_data_ETL_aggregation.py

Description & goal: A DAG to process patient data obtained from different wearable sensors and then push the processed data into a postgres instance

Remarks: To be able to run the dag, connections to the postgres instance and s3 bucket must be added. In addition, the referenced variables must be added to Airflow's environment

Author: Yasmine Mohamed

#### patient2_glucose_data_ETL.py

Description & goal: A DAG to process patient blood glucose level measurements obtained from a wearable device and then push the processed data into a postgres instance

Remarks: To be able to run the dag, connections to the postgres instance and s3 bucket must be added. In addition, the referenced variables must be added to Airflow's environment

Author: Yasmine Mohamed

#### d1namo_DW_lambda_function.py

Description and goal: A lambda function that:
1. establishes connections to data lake postgres instance, data warehouse postgres instance and s3 bucket.
2. executes sql query that joins two tables from the data lake and push the combined data into:
  a. the data warehouse to easily visualize and validate the data
  b. the s3 bucket, to easily connect to amazon SageMaker, an AWS machine learning tool

Remarks: For the lambda function to work, the postgres database credentials have to be added to the lambda function's environment. Additionally, cloud 9 layer with the required packages (pandas, requests, psycopg2, sqlalchemy) had to be added to the lambda function.

Author: Yasmine Mohamed

#### d1namo_ML_SageMaker.ipynb

Description and goal: A SageMaker studio notebook to develop machine learning models to detect hypoglycemia based on sensor data. 

Remarks: The credentials for the data warehouse need to be provided to export the data.

Author: Yasmine


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

#### ClinicalTrialsAPI.py

Description & goal: performs a monthly call to the Clinical Trials API and pushes the data to the data lake

Remarks: or the lambda function to work, the postgres database credentials have to be added to the lambda function's environment. Additionally, cloud 9 layer with the required packages had to be added to the lambda function.

Author: Yasmine Mohamed

#### ClinicalTrials_DW_lambda_function.py

Description & goal: 
1. Establishes connections to the data lake and data warehouse postgres instances
2. Queries the data lake's schema (ClinicalTrialsAPI) that has full information about diabetes clinical trials retrieved through the clinical trials API
3. Creates a tables with only recent trials (last 4 years) in the data warehouse

Remarks: For the lambda function to work, the postgres database credentials have to be added to the lambda function's environment. Additionally, cloud 9 layer with the required packages had to be added to the lambda function.

Author: Yasmine Mohamed

 
## Static data

#### Static_data.ipynb

Description & goal: Migrate data from the API of each source into a single S3 AWS folder.

Remarks: The AWS credentials have to be added, in order for the connection to the s3 bucket to be established

Author: Carla Frege

