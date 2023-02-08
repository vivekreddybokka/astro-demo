from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import boto3

default_args = {
    'owner': 'me',
    'start_date': datetime(2021, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'list_objects_from_s3',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
)

def list_objects_from_bucket(**kwargs):
    s3 = boto3.client('s3')
    bucket_name = kwargs['params']['bucket_name']
    objects = s3.list_objects(Bucket=bucket_name)
    if 'Contents' in objects:
        kwargs['ti'].xcom_push(key='objects', value=objects['Contents'])
        return True
    else:
        return False

list_objects = PythonOperator(
    task_id='list_objects',
    python_callable=list_objects_from_bucket,
    op_kwargs={'bucket_name': 'my-bucket'},
    provide_context=True,
    dag=dag
)

def check_if_objects_exist(**kwargs):
    objects = kwargs['ti'].xcom_pull(key='objects')
    if objects:
        return 'copy_objects'
    else:
        print('Bucket is empty')
        return 'end_task'

check_objects_existence = PythonOperator(
    task_id='check_objects_existence',
    python_callable=check_if_objects_exist,
    provide_context=True,
    dag=dag
)

def copy_objects(**kwargs):
    s3 = boto3.client('s3')
    objects = kwargs['ti'].xcom_pull(key='objects')
    for obj in objects:
        s3.copy_object(Bucket='destination-bucket', CopySource={'Bucket': 'my-bucket', 'Key': obj['Key']}, Key=obj['Key'])

copy_objects = PythonOperator(
    task_id='copy_objects',
    python_callable=copy_objects,
    provide_context=True,
    dag=dag
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=lambda: None
)