from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from script.test_oop import DataAgregation

with DAG(
    'aggr_count',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retries_delay': timedelta(minutes=3),
    },
    description='Описание',
    schedule_interval='0 * * * *',  # Запуск каждый час
    start_date=datetime(2024, 9, 23, 10, 40),
    catchup=False,
) as dag:
    @task(task_id='my_dag')
    def new_func(exec_date: str):
        DataAgregation.run(exec_date)

    new_func(' {{ ds }} ')
