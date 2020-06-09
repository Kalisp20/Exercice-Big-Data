# airflow related
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
# other packages
from datetime import datetime
from datetime import timedelta

from airflow.contrib.operators.ssh_operator import SSHOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
        dag_id='my_fucking_dag3',
    description='Big Data DAG',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5))


t1_bash="""
    spark-submit --conf "spark.pyspark.python=/usr/bin/python3" traitement_csv.py
    """ 


connect_ssh= SSHOperator(task_id='connect_ssh',
                      ssh_conn_id= 'ssh_default',
                      command= t1_bash,
                      dag=dag,
                      )



connect_ssh
