from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from datetime import datetime
import pandas as pd
import boto3

dag = DAG(
    'etl_dynamodb_a_s3',
    description='Proceso ETL de DynamoDB a S3',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

def get_aws_credentials():
    conn = BaseHook.get_connection("aws_credentials")
    aws_credentials = {
        "access_key": conn.login,
        "secret_access_key": conn.password,
        "session_token": conn.extra_dejson.get("aws_session_token"),
    }
    return aws_credentials

@task(dag=dag)
def extract_from_dynamodb():
    aws_credentials = get_aws_credentials()
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=aws_credentials["access_key"],
        aws_secret_access_key=aws_credentials["secret_access_key"],
        aws_session_token=aws_credentials["session_token"]
    )
    table = dynamodb.Table('t_airflow')
    response = table.scan()
    items = response['Items']
    df = pd.DataFrame(items)
    df.to_csv('/tmp/extracted_data.csv', index=False)
    print(df)
    print("Tarea 1 - Extraer datos de DynamoDB")

@task(dag=dag)
def transform_data():
    df = pd.read_csv('/tmp/extracted_data.csv')
    df.columns = [col.lower() for col in df.columns]
    df.to_csv('/tmp/transformed_data.csv', index=False)
    print(df)
    print("Tarea 2 - Transformar a minúsculas los nombres de columnas")

@task(dag=dag)
def load_to_s3():
    aws_credentials = get_aws_credentials()
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_credentials["access_key"],
        aws_secret_access_key=aws_credentials["secret_access_key"],
        aws_session_token=aws_credentials["session_token"]
    )
    bucket_name = 'mybucketcloudcomputing'
    file_name = '/tmp/transformed_data.csv'
    s3.upload_file(file_name, bucket_name, 'transformed_data.csv')
    print("Tarea 3 - Cargar csv a S3")

extract_task = extract_from_dynamodb()
transform_task = transform_data()
load_task = load_to_s3()

extract_task >> transform_task >> load_task
