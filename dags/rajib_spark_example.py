import time
from pprint import pprint
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.models import Variable

args = {
    'owner': 'Rajibul Islam',
    'start_date': datetime(2022, 8, 10),
    'retries': 1,
    'domain': 'bigdata_classs',
    'retry_delay': timedelta(minutes=5)
}

dag_id = 'rajib_spark_example'
dag = DAG(
    dag_id=dag_id,
    default_args=args,
    catchup=True,
    schedule_interval="0 18 * * *",  # Every day at 9 AM EST
    max_active_runs=1,
    tags=['rajib']
)

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


awsAccessKeyId = Variable.get("rajib_aws_access_key")
awsSecretAccessKey = Variable.get("rajib_aws_secret_key")
process_data_cmd = """set -e
                    cd {spark_folder_path}
                    cat spark4airflow.py | ssh root@localhost -p2244 python3 - {awsAccessKeyId} \
                    {awsSecretAccessKey} {top_n} {s3_input_path} {s3_output_path}
                    """.format(spark_folder_path='/var/volshares/airflow/dags/helper/spark/',
                               awsAccessKeyId=awsAccessKeyId,
                               awsSecretAccessKey=awsSecretAccessKey, top_n=10,
                               s3_input_path='s3n://rajib-sf-ingest/sale-data/csv/customers_v2.csv',
                               s3_output_path='s3n://rajib-sf-data/sale-data/json/state_by_customer/')
print('process_data_cmd:',process_data_cmd)

process_data = SSHOperator(
    task_id='process_data',
    ssh_hook=sshHook,
    command=process_data_cmd,
    depends_on_past=True,
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
