from airflow import DAG, XComArg
from airflow.decorators import task
import pendulum
from airflow.providers.amazon.aws.operators.s3 import (
    S3CopyObjectOperator, S3ListOperator, S3DeleteBucketOperator
)
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

S3_SOURCE_BUCKET = "vivek-astro-login"
S3_DESTINATION_BUCKET_PRIMARY = "vivek-astro-login/primary"
S3_DESTINATION_BUCKET_SECONDARY = "vivek-astro-login/secondary"

with DAG(
    dag_id="primary",
    start_date=pendulum.datetime(2023, 2, 5, tz="UTC"),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
):
    list_files_in_source_bucket = S3ListOperator(
        task_id = list_files_in_source_bucket,
        aws_conn_id= "aws_default",
        bucket=S3_SOURCE_BUCKET
    )
    @task
    def read_keys_from_s3(source_key_list):
        "rFetch files from source bucket"
        s3_hook = S3Hook(aws_conn_id='aws_default')
        print(source_key_list)
        content_list = []
        for key in source_key_list:
            file_content = s3_hook.read_key(
                key=key,
                bucket_name=S3_SOURCE_BUCKET
            )
            content_list.append(file_content)

        return content_list
    content_list = read_keys_from_s3(XComArg(list_files_in_source_bucket))

    @task
    def test_if_integer(content_list):
        """test each file name if contains integer"""
        destination_list = []
        for file_content in content_list:
            if isinstance(file_content, int):
                destination_list.append(S3_DESTINATION_BUCKET_PRIMARY)
            else:
                destination_list.append(S3_DESTINATION_BUCKET_SECONDARY)
        return destination_list
    
    # create a list of destinations based on centent in the files

    dest_key_list = test_if_integer(content_list)