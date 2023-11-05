import requests
import pandas as pd
import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from io import StringIO

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 12),
    'retries': 1,
}

dag = DAG('supermarket_sales_dag', default_args=default_args, schedule_interval='@hourly', catchup=False)


def get_s3_client(endpoint_url, aws_access_key_id, aws_secret_access_key):
    s3 = boto3.client('s3', endpoint_url=endpoint_url,
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key,
                      config=boto3.session.Config(signature_version='s3v4'))
    return s3


def save_df_to_s3(df, bucket, key, s3):
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
        print(f"{key} saved to S3 bucket {bucket}")
    except Exception as e:
        print(f"Error saving {key} to S3 bucket {bucket}: {e}")


def process_supermarket_sales():
    endpoint_url = 'http://minio:9000'
    aws_access_key_id = 'cagri'
    aws_secret_access_key = '35413541'
    source_file_url = 'http://nginx:80/data/'

    s3 = get_s3_client(endpoint_url, aws_access_key_id, aws_secret_access_key)

    today = datetime.now().strftime("%Y%m%d")

    for hour in range(24):
        current_hour = f"{hour:02}"
        data_source_url = f"{source_file_url}hour_{current_hour}_supermarket_sales.csv"
        response = requests.get(data_source_url)

        if response.status_code == 200:
            data = response.text
            df = pd.read_csv(StringIO(data))
            s3_key = f"supermarket_sales/{today}_{current_hour}/hour_{current_hour}_supermarket_sales.csv"
            save_df_to_s3(df, bucket='airflow-homework-1', key=s3_key, s3=s3)
        else:
            s3_key = f"supermarket_sales/{today}_{current_hour}/hour_{current_hour}_supermarket_sales.csv"
            empty_df = pd.DataFrame(columns=['column1', 'column2'])
            save_df_to_s3(empty_df, bucket='airflow-homework-1', key=s3_key, s3=s3)


process_sales_task = PythonOperator(
    task_id='process_sales_task',
    python_callable=process_supermarket_sales,
    dag=dag,
)

process_sales_task
