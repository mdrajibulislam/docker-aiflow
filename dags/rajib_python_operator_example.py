import time
from pprint import pprint
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
# from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'rajib',
}

args = {
    'owner': 'Rajibul Islam',
    'start_date': datetime(2022, 8, 10),
    'retries': 1,
    'domain': 'bigdata_classs',
    'retry_delay': timedelta(minutes=5)
}
dag_id='rajib_python_operator_example'
dag = DAG(
    dag_id=dag_id,
    default_args=args,
    catchup=True,
    schedule_interval="0 18 * * *",  # Every day at 9 AM EST
    max_active_runs=1
)

dag.doc_md = __doc__
start = DummyOperator(task_id='start', dag=dag)  # Kickoff DAG


def _process_data(**kwargs):
    """
    doc
    """
    print('======================')
    print('process data')
    print('======================')


def _end_task(**kwargs):
    """
    doc
    """
    print('======================')
    print('End task')
    print('======================')


# clean task is commented
process_data = PythonOperator(
    task_id='process_data',
    provide_context=True,
    python_callable=_process_data,
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
