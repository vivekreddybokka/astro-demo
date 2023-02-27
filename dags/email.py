from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'example_email_operator',
    default_args=default_args,
    schedule_interval='@once',
)

send_email = EmailOperator(
    task_id='send_email',
    to='recipient@example.com',
    subject='Airflow DAG Notification',
    html_content='This is a notification email sent from Airflow!',
    dag=dag,
    conn_id='smtp_connection',
)

send_email
