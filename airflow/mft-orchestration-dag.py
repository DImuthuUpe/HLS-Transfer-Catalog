from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

def fetch_pending():
    # curl --location --request GET 'http://127.0.0.1:5000/catalogue/?transfer_status=IN_PROGRESS'
    response = requests.get("http://192.168.122.1:5000/catalogue/?transfer_status=IN_PROGRESS")
    in_progress_files = response.json()
    print("Fetching pending transfers")

def get_transfer_status_from_mft():
    print("Fetching transfer status from mft")

def update_completed_transfers():
    # curl --location --request PATCH 'http://127.0.0.1:5000/catalogue/c266dc7b9fad4d64aaa7d103b6f0af09/'
    # --header 'Content-Type: application/json' \
    # --data-raw '{
    #   "name": "test",
    #  "transfer_status": "COMPLETED"
    #  }'
    print("Updating completed transfers")

def fetch_unprocessed(**context):
    response = requests.get("http://192.168.122.1:5000/catalogue/?transfer_status=NOT_STARTED")
    print(response.json())
    context['ti'].xcom_push(key="to_transfer", value = response.json())
    print("Fetching unprocessed data points")

def submit_to_mft(**context):
    file_list = context['ti'].xcom_pull(key="to_transfer")
    print("Submitting to MFT ", file_list)

def update_submitted_transfers():
    # curl --location --request PATCH 'http://127.0.0.1:5000/catalogue/c266dc7b9fad4d64aaa7d103b6f0af09/'
    # --header 'Content-Type: application/json' \
    # --data-raw '{
    #   "name": "test",
    #  "transfer_status": "IN_PROGRESS"
    #  }'
    print("Updating the transfer status")

with DAG(dag_id="hls-nasa-hot-storage", start_date=datetime(2021,1,1), schedule_interval="@hourly", catchup=False) as dag:
    task1 = PythonOperator(task_id="fetch_pending", python_callable=fetch_pending)
    task2 = PythonOperator(task_id="get_transfer_status_from_mft", python_callable=get_transfer_status_from_mft)
    task3 = PythonOperator(task_id="update_completed_transfers", python_callable=update_completed_transfers)
    task4 = PythonOperator(task_id="fetch_unprocessed", python_callable=fetch_unprocessed)
    task5 = PythonOperator(task_id="submit_to_mft", python_callable=submit_to_mft)
    task6 = PythonOperator(task_id="update_submitted_transfers", python_callable=update_submitted_transfers)
    
task1 >> task2 >> task3 >> task4 >> task5 >> task6