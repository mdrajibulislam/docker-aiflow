import time
from pprint import pprint
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
# from airflow.contrib.operators.ssh import SSHOperator # before 2
from airflow.contrib.hooks.ssh_hook import SSHHook
# from airflow.operators.python_operator import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
# from airflow.sensors import s3_key_sensor
from airflow.sensors.s3_key_sensor import S3KeySensor

args = {
    'owner': 'Rajibul Islam',
    'start_date': datetime(2022, 8, 10),
    'retries': 1,
    'domain': 'bigdata_classs',
    'retry_delay': timedelta(minutes=5)
}

dag_id='rajib_s3_keysensor_example'
dag = DAG(
    dag_id=dag_id,
    default_args=args,
    catchup=True,
    schedule_interval="0 18 * * *",  # Every day at 9 AM EST
    max_active_runs=1
)

dag.doc_md = __doc__
start = DummyOperator(task_id='start', dag=dag)  # Kickoff DAG

def _end_task(**kwargs):
    """
    doc
    """
    print('======================')
    print('End task')
    print('======================')
bucket_name='rajib-sf-ingest'
key = 'sale-data/csv/customers_v2.csv'
check_if_input_file_exists = S3KeySensor(
    bucket_name=bucket_name,
    bucket_key=key,
    timeout=60,
    poke_interval=10,
    task_id="check_if_input_file_exists",
    aws_conn_id='rajib-s3',
    verify=False,
    dag=dag
)

# clean task is commented
end_task = PythonOperator(
    task_id='end_task',
    provide_context=True,
    python_callable=_end_task,
    dag=dag
)

start >> check_if_input_file_exists >> end_task
