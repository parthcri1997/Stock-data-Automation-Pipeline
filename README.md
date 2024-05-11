# Stock-data-Automation-Pipeline
 An automated data pipeline in Python language on AWS to retrieve and process end-of-day stock data, using EC2, S3, Glue, Lambda, and Athena. Implemented ETL processes and set up email notifications for job status, with an API Gateway for user access to specific stock prices on demand, and all resources are provisioned using CloudFormation.

### Steps
1) First create empty S3 bucket and replace it with the value Your_S3_Bucket_Name in the cloudformation yml template.
2) Upload the 2 lambda code zip named lambda_function_for_getting_stock_data.zip and trigger_glue_job.zip. 
3) Now upload the cloudformation yaml and it will create all the resources required.
4) Connect to EC2 and install python 3.11,pip command and then run pip3.11 install -r requirements.txt for installing the required libraries.
5) Configure aws_access_key_id and aws_secret_access_key using aws configure command on EC2. I have set region as us-east-1 and the default output type as json.
6) Run command aws configure set aws_session_token "Get the session token".
7) Upload the fetch_data_from_api.py file on EC2.
8) Run the fetch_data_from_api.py file on EC2 using the command python3.11 fetch_data_from_api.py

After completing the above steps, the script will store the data fetched from api on S3 bucket.
