from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import boto3
from airflow.hooks.base_hook import BaseHook
from io import StringIO

aws_conn = BaseHook.get_connection('aws_default')
s3 = boto3.client('s3', aws_access_key_id=aws_conn.login, aws_secret_access_key=aws_conn.password)

bucket = "hexaware-inbound-bucket"

#Define default_args dictionary to pass to the DAG
default_args = {
    'owner': 'me',
    'start_date': datetime(2023, 1, 24),
    'depends_on_past': False,
    'retries': 0,
    'schedule_interval': '@daily',
}

# def s3_connect(**kwargs):
#     # Connect to AWS using the default connection
#     aws_conn = BaseHook.get_connection('aws_default')
#     # s3 = boto3.client('s3', aws_access_key_id=aws_conn.login, aws_secret_access_key=aws_conn.password)

#Instantiate a DAG
dag = DAG(
    'data_conversion', default_args=default_args)

def sort_data(**kwargs):
  
  #Reading the csv file as a Pandas DataFrame. The file can be an excel sheet. Other libraries exist for different filetypes like pdf,docx,txt
  
  read_file = s3.get_object(Bucket=bucket, Key='unprocessed/People_data.csv')
  original_data = pd.read_csv(read_file['Body'])
  #original_data=pd.read_csv('/usr/local/airflow/dags/People_data.csv')
  key="Birth Date"
  #Checking datatype of key here. If string convert to datetime object for sorting
#   s3_connect()
  if type(original_data.iloc[0][key])==str:
    working_data=original_data.copy()
    working_data[key]=pd.to_datetime(working_data[key])
    sorted_data=working_data.sort_values(by=key)
    print ("This is sorted data", sorted_data)
    
    sorted_data.to_csv('sorted_data.csv',index=False)
    csv_buffer = StringIO()
    sorted_data.to_csv(csv_buffer)

    response = s3.put_object(
    Body=csv_buffer.getvalue(),
    Bucket=bucket,
    Key='processed/converted.csv',
)
    print ("done writing data to sorted_data.csv")
 
 # Define the task using PythonOperator
read_and_write_task = PythonOperator(
    task_id='sort_csv',
    python_callable=sort_data,
    dag=dag)

# Define the DAG structure
read_and_write_task

