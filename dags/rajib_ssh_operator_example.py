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


args = {
    'owner': 'Rajibul Islam',
    'start_date': datetime(2022, 8, 10),
    'retries': 1,
    'domain': 'bigdata_classs',
    'retry_delay': timedelta(minutes=5)
}

dag_id='rajib_ssh_operator_example'
dag = DAG(
    dag_id=dag_id,
    default_args=args,
    catchup=True,
    schedule_interval="0 18 * * *",  # Every day at 9 AM EST
    max_active_runs=1
)

import pathlib
project_path = pathlib.Path(__file__).resolve().parent.parent
print('project_path',project_path)
# def complete_path(path):
#     with open(str(project_path) + '/' + path, "r") as file:
#         return file.read()

dag.doc_md = __doc__
start = DummyOperator(task_id='start', dag=dag)  # Kickoff DAG
sshHook = SSHHook(ssh_conn_id="master-node")
def _end_task(**kwargs):
    """
    doc
    """
    print('======================')
    print('End task')
    print('======================')

process_data = SSHOperator(
    task_id='process_data',
    ssh_hook=sshHook,
    command="""
            cd {{params.base_path}}
            ls -l
            """,
    depends_on_past=True,
    params={'base_path': project_path},
    dag=dag
)

# clean task is commented
end_task = PythonOperator(
    task_id='end_task',
    provide_context=True,
    python_callable=_end_task,
    dag=dag
)

start >> process_data >> end_task
