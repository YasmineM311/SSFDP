# SSFDP
This a project to create a data lake and data warehouse for a hypothetical high tech healthcare company that aims at developing new technologies to improve the quality of life of diabetic patients

## Sources
### Springer Nature API
*SpringerAPI_lambda_function1.py* 

Description & goal: Lambda function triggered weekly to query the Springer API, with 'diabetes AND wearables' as a search keyword. It aims to retreive the most recent publications about using wearable technology in diabetes management

*SpringerAPI_lambda_function2.py*

Description & goal: Lambda function triggered weekly to query the Springer API, with 'diabetes' as a search keyword. It aims to retreive the most recent publications about diabetes in general

### D1namo 
- Migration of data to an S3 bucket: 
https://github.com/YasmineM311/SSFDP/blob/main/migrating%20the%20d1namo%20dataset%20to%20an%20s3%20bucket.ipynb
- DAG 1, for raw sensor data (accelerometer): https://github.com/YasmineM311/SSFDP/blob/main/patient2_accel_data_ETL_aggregation.py
- DAG 2, for summarized sensor data: https://github.com/YasmineM311/SSFDP/blob/main/patient2_summary_data_ETL_aggregation.py
- DAG 3, for continuous glucose monitoring data: https://github.com/YasmineM311/SSFDP/blob/main/patient2_glucose_data_ETL.py 

### Clinical Trials
- Docker airflow composition: https://github.com/YasmineM311/SSFDP/blob/main/docker-compose.yaml
- ClinicalTrials.gov API request script: https://github.com/YasmineM311/SSFDP/blob/main/client.py AND
 https://github.com/YasmineM311/SSFDP/blob/main/utils.py 
- ClinicalTrials.gov API DAG: https://github.com/YasmineM311/SSFDP/blob/main/results_dag.py 
 
### Static data
Migration of data to an S3 bucket: https://github.com/YasmineM311/SSFDP/blob/main/Static_data.ipynb
