from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from script.aggregate_count import DataAggregator

with DAG(
        'aggregation_count',
        default_args={
            'depends_on_past': False,
            'retries': 1,
            'retries_delay': timedelta(minutes=3),
        },
        description='Вычисление агрегированных показателей за 7 дней',
        schedule_interval='0 7 * * *',
        start_date=datetime(2024, 9, 25, 5, 0),
        catchup=False,
) as dag:
    @task(task_id='agg_dag')
    def count_func(exec_date: str):
        required_date = datetime.strptime(exec_date.strip(), "%Y-%m-%d")
        DataAggregator(date_all=required_date).create_data()


    count_func(' {{ ds }} ')
