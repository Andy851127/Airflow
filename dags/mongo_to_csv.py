from pymongo import MongoClient
from airflow import DAG
import airflow
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from datetime import timedelta

# 定義連接MongoDB的函數
def connect_to_mongodb():
    username = "root"  # 替换为你的MongoDB用户名
    password = "root"  # 替换为你的MongoDB密码
    host = "192.168.214.1"
    port = 27018
    authSource  = "admin"  # 默认的身份验证数据库是admin，替换为你的身份验证数据库

    # Connect to MongoDB
    client = MongoClient(
                      host,
                      port,
                      username=username,
                      password=password,
                      authSource=authSource
                    )  # assuming your MongoDB container is named 'mongodb'
    # Get the database
    db = client['wikimedia']

    # Get the collection
    collection = db['wikimedia_collection']

    return collection

def get_mongo_datas():
    collection = connect_to_mongodb()
    fields = {
        'type': 1,
        'title': 1,
        'title_url': 1,
        'comment': 1,
        'timestamp': 1,
        'user': 1,
        'bot': 1,
        '_id': 0  # 不包括 _id
    }
    mongodb_datas = list(collection.find({}, fields))
    return mongodb_datas

# 定義將資料轉換為 CSV 的函數
def convert_to_csv(**kwargs):
    # 使用 xcom_pull 提取数据
    ti = kwargs['ti']
    mongodb_datas = ti.xcom_pull(task_ids='get_mongo_datas')
    df = pd.DataFrame(mongodb_datas)
    print(df)
    df.to_csv("/opt/airflow/dags/csv/wikimedia.csv", index=False)
    
# 定義 Airflow 的 DAG
with DAG(
    dag_id='mongodb_to_csv',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(2),
    tags=['andychen'],
    catchup=False,
) as dag:

    # 定義連接 MongoDB 的任務
    get_mongo_datas_task = PythonOperator(
        task_id="get_mongo_datas",
        python_callable=get_mongo_datas
    )

    # 定義將資料轉換為 CSV 的任務
    convert_to_csv_task = PythonOperator(
        task_id="convert_to_csv",
        python_callable=convert_to_csv,
        # op_kwargs={"mongodb_datas": "{{ task_instance.xcom_pull(task_ids='get_mongo_datas') }}"}
        provide_context=True
    )

    # 定義任務之間的依賴關係
    get_mongo_datas_task >> convert_to_csv_task
    # get_mongo_datas_task
