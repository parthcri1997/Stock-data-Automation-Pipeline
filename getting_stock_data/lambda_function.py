import json
import boto3
import csv
import pandas as pd
def run_query(athena_client, query, bucket_name):
    print("Executing query")
    response = athena_client.start_query_execution(QueryString=query,
                                                   QueryExecutionContext={'Database': 'stock_data_db'},
                                                   ResultConfiguration={
                                                       'OutputLocation': 's3://{}/athenaOutput/'.format(bucket_name)})
    print("Response of run query function:", response)
    return response


def query_status(athena_client, athena_query_id):
    resp = ["FAILED", "SUCCEEDED", "CANCELLED"]
    response = athena_client.get_query_execution(QueryExecutionId=athena_query_id)
    while response["QueryExecution"]["Status"]["State"] not in resp:
        response = athena_client.get_query_execution(QueryExecutionId=athena_query_id)
    return response["QueryExecution"]["Status"]


def athena_query_execute(athena_client, query, bucket_name, s3_client):
    print("Starting query:", query)
    query_execution = run_query(athena_client, query, bucket_name)
    query_state = query_status(athena_client, query_execution['QueryExecutionId'])
    print("Athena query state:", query_state)
    # print("Query execution id:", query_execution['QueryExecutionId'])
    file_name = "athenaOutput/" + query_execution['QueryExecutionId'] + '.csv'
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    # print(type(obj['Body']))
    # csv_reader = csv.reader(obj['Body'].decode('utf-8'))
    # json_data = []
    # for row in csv_reader:
    #     print(row)
    #     json_obj={}
    #     for i,value in enumerate(row):
    #         json_obj[header[i]] = value
    #     json_data.append(json_obj)
    # return json.dumps(json_data)
    return pd.read_csv(obj['Body'])


def lambda_handler(event, context):
    print(event)
    query = f"select * from stock_data_db.stock_data where symbol = '{event['symbol']}' and date = cast(date_parse('{event['date']}','%Y-%m-%d') as date);"
    s3_client = boto3.client('s3')
    athena_client = boto3.client('athena')
    bucket_name = 'stockdatatermassignment5409'
    query_output = athena_query_execute(athena_client,query,bucket_name,s3_client)
    # print("Query output:",query_output,type(query_output))
    # print(json.loads(query_output.to_json(orient='records')),type(json.loads(query_output.to_json(orient = 'records'))))
    # return json.loads(query_output.to_json(orient='records'))[0]
    return {
    "isBase64Encoded": False,
    "statusCode": 200,
    "headers": { "X-Amz-Invocation-Type": 'Event' },
    "body": json.loads(query_output.to_json(orient='records'))[0]
    }