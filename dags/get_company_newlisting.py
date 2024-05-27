from pymongo import MongoClient
from airflow import DAG
import airflow
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json
import pandas as pd
import requests
from datetime import timedelta

# 定義連接MongoDB的函數
def get_company_newlisting():
    json_path = "/opt/airflow/dags/json/"
    name = "company_newlisting.json"
    url = 'https://openapi.twse.com.tw/v1/company/newlisting'
    company_newlisting_datas = requests.get(url)
    company_newlisting_datas = company_newlisting_datas.json()
    
    with open(json_path + name,'w',encoding= 'utf-8') as json_file:
        json.dump(company_newlisting_datas, json_file, ensure_ascii=False, indent=4)
    
    return company_newlisting_datas

# 定義 Airflow 的 DAG
with DAG(
    dag_id='get_company_newlisting',
    schedule_interval='0 0 * * *',  # 每天凌晨 12:00
    start_date=days_ago(2),
    tags=['andychen'],
    catchup=False,
) as dag:

    # 定義連接 MongoDB 的任務
    get_company_newlisting_task = PythonOperator(
        task_id="get_mongo_datas",
        python_callable=get_company_newlisting
    )

    # 定義任務之間的依賴關係
    get_company_newlisting_task 
    # get_mongo_datas_task
