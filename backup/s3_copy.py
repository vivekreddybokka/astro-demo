import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import boto3

def list_and_copy_files(**kwargs):
    # Connect to AWS using the default connection
    aws_conn = BaseHook.get_connection('aws_default')
    s3 = boto3.client('s3', aws_access_key_id=aws_conn.login, aws_secret_access_key=aws_conn.password)

    # List all files in the folder
    files = s3.list_objects(Bucket='vivek-new-1', Prefix='transfer/')['Contents']



    # Copy each file to the archiving folder
    for file in files:
        s3.copy_object(Bucket='vivek-new-1', CopySource={'Bucket': 'vivek-new-1', 'Key': file['Key']},
                       Key='archived/' + file['Key'].split('/')[-1])
        s3.delete_object(Bucket='vivek-new-1', Key=file['Key'])

dag = DAG(dag_id='list_and_copy_files_dag',
          default_args={'start_date': airflow.utils.dates.days_ago(1)})

list_and_copy_task = PythonOperator(task_id='list_and_copy_files_task',
                                    python_callable=list_and_copy_files,
                                    provide_context=True,
                                    dag=dag)

list_and_copy_task
