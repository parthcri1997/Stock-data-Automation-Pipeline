import datetime
import sys
from awsglue.utils import getResolvedOptions
import boto3
import pandas as pd
import json


# https://stackoverflow.com/questions/57166753/querying-athena-tables-in-aws-glue-python-shell : athena query taken from here
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
    print("Query execution id:", query_execution['QueryExecutionId'])
    file_name = "athenaOutput/" + query_execution['QueryExecutionId'] + '.csv'
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    return pd.read_csv(obj['Body'])


# https://stackoverflow.com/questions/40995251/reading-an-json-file-from-s3-using-python-boto3 : taken from here.
def read_json_from_s3(s3_client, bucket_name, file_path):
    result = s3_client.get_object(Bucket=bucket_name, Key=file_path)
    json_data = json.loads(result["Body"].read().decode())
    print(type(json_data))
    return json_data


args = getResolvedOptions(sys.argv,
                          ['job_name',
                           's3_bucket',
                           's3_source_file_path',
                           's3_dest_file_path'
                           ])
print("Bucket:", args['s3_bucket'])
print("S3 source file path:", args['s3_source_file_path'])
s3_client = boto3.client('s3')
athena_client = boto3.client('athena')
load_full_data = False
print("File name:", args['s3_source_file_path'].split("/")[-1])
file_name = args['s3_source_file_path'].split("/")[-1]
print("Getting symbol name")
symbol = file_name.split("_")[0]
print(symbol)
athena_max_date_query = f"select max(date) as max_date from stock_data_db.stock_data where symbol='{symbol}';"
json_data = read_json_from_s3(s3_client, args['s3_bucket'], args['s3_source_file_path'])
#print("Json data:", json_data)
print("Json data:", json_data["data"])
# athena_max_date = datetime.datetime.strptime("2023-09-11T00:00:00.000Z","%Y-%m-%dT%H:%M:%S.%fZ")
athena_max_date_query_result = athena_query_execute(athena_client, athena_max_date_query, args['s3_bucket'], s3_client)
if athena_max_date_query_result.empty:
    athena_max_date = datetime.datetime.strptime("2023-09-11T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
    load_full_data = True
else:
    athena_max_date = datetime.datetime.strptime(athena_max_date_query_result["max_date"].iloc[0], "%Y-%m-%d")
print("Athena max date:", athena_max_date)
latest_date = datetime.datetime.strptime(json_data["data"][0]["date"], "%Y-%m-%dT%H:%M:%S.%fZ")
if abs(latest_date - athena_max_date).days > 0:
    if load_full_data:
        for record in json_data["data"]:
            df = pd.DataFrame()
            df = df.append({
                "Date": datetime.datetime.strptime(record["date"],"%Y-%m-%dT%H:%M:%S.%fZ").date(),
                "Symbol": symbol,
                "Open": record["open"],
                "High": record["high"],
                "Low": record["low"],
                "Close": record["close"],
                "Volume": record["volume"]
            }, ignore_index=True)
            dest_s3_path = 's3://' + args['s3_bucket'] + '/' + 'csv/'  + symbol + "_" + str(datetime.datetime.strptime(record["date"],"%Y-%m-%dT%H:%M:%S.%fZ").date()) + ".csv"
            print("Dest s3 path:", dest_s3_path)
            df.to_csv(dest_s3_path, index=False)
            print("All Data written succesfully to s3.")
    else:
        df = pd.DataFrame()
        print(latest_date, json_data["data"][0]["open"])
        print(json_data["data"][0]["high"], json_data["data"][0]["low"], json_data["data"][0]["close"],
              json_data["data"][0]["volume"])
        df = df.append({
            "Date": latest_date,
            "Symbol": symbol,
            "Open": json_data["data"][0]["open"],
            "High": json_data["data"][0]["high"],
            "Low": json_data["data"][0]["low"],
            "Close": json_data["data"][0]["close"],
            "Volume": json_data["data"][0]["volume"]
        }, ignore_index=True)
        dest_s3_path = 's3://' + args['s3_bucket'] + '/' + args['s3_dest_file_path'] + file_name.replace(".json",
                                                                                                         ".csv")
        print("Dest s3 path:", dest_s3_path)
        df.to_csv(dest_s3_path, index=False)
        print("Data written succesfully to s3.")

"""
            json example from stock data which will be stored in json_data variable above.
            {
                "meta": {
                    "date_from": "2023-09-11",
                    "date_to": "2023-09-12",
                    "max_period_days": 180
                },
                "data": [
                    {
                        "date": "2023-09-12T00:00:00.000Z",
                        "open": 179.49,
                        "high": 180.11,
                        "low": 174.84,
                        "close": 176.29,
                        "volume": 1454605
                    },
                    {
                        "date": "2023-09-11T00:00:00.000Z",
                        "open": 180.08,
                        "high": 180.3,
                        "low": 177.35,
                        "close": 179.39,
                        "volume": 909570
                    } ] }
            """