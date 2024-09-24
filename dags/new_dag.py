from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from script.aggregate_count import DataAggregator

with DAG(
        'aggr_count',
        default_args={
            'depends_on_past': False,
            'retries': 1,
            'retries_delay': timedelta(minutes=3),
        },
        description='Вычисление агрегированных показателей за 7 дней',
        schedule_interval='0 7 * * *',
        start_date=datetime(2024, 9, 23, 10, 0),
        catchup=False,
) as dag:
    @task(task_id='my_dag')
    def new_func(exec_date: str):
        required_date = datetime.strptime(exec_date.strip(), "%Y-%m-%d")
        DataAggregator(date_all=required_date).create_data()


    new_func(' {{ ds }} ')
