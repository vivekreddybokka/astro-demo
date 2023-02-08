from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG("get_price_Marriott", start_date=datetime(2023, 1, 1), schedule_interval="@daily", 
    catchup=False) as dag:


    @task
    def extract(stock):
        print ("Hello from extract", stock)
        return stock

    @task
    def process(stock):
        print ("Hello from process", stock)
        return stock

    @task
    def email(stock):
        print ("Hello from email", stock)
        return stock
    
    email(process(extract(125)))