from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
import sys

sys.path.append('')
from mininal_script import act_count

with DAG(
    'aggr_count',
    default_args= {
        'depends_on_past': False,
        'retries': 1,
        'retries_delay': timedelta(minutes= 3)
    },
    description='Описание',
    schedule_interval= '@daily',
    start_date= datetime(2024, 9, 20, 15, 0),
    cathup= False
) as dag:
    @task(task_id='my_dag')
    def new_func():
        res = act_count()


    new_func()