import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException
from dataclasses import dataclass

# @dataclass
# class AggregatorConfig:


class DataAggregator:

    def __init__(self, date_all: datetime, d: int = 7, dir_name_in: str = 'input',
                 dir_name_out: str = 'output', inter_dir: str = 'inter'):
        self.date_all = date_all
        self.begin_date = date_all - timedelta(days=d)
        self.dir_name_in = dir_name_in
        self.dir_name_out = dir_name_out
        self.d = d
        self.inter_dir = inter_dir
        self.spark = SparkSession.builder.appName("DataAggregation").getOrCreate()

    def check_files_exist(self, dir: str, day: datetime) -> bool:
        input_file_path = os.path.join(dir, f'{day.strftime("%Y-%m-%d")}.csv')
        return os.path.exists(input_file_path)

    def save_data(self, dir: str, dt, day: datetime):
        output_path = os.path.join(dir, f'{day.strftime("%Y-%m-%d")}.csv')
        if not os.path.exists(dir):
            os.makedirs(dir)
        dt.write.csv(output_path, header=True, mode="overwrite")

    def create_counted_data(self, dt):
        data = self.count_actions(dt)
        return data

    def create_data(self):
        existed_files = []
        data = None

        for index in range(self.d):
            day = self.begin_date + timedelta(days=index)
            if not self.check_files_exist(self.dir_name_in, day):
                raise AirflowException("File not found!")
            else:
                if not self.check_files_exist(self.inter_dir, day):
                    file_path = os.path.join(self.dir_name_in, f'{day.strftime("%Y-%m-%d")}.csv')
                    day_dt = self.spark.read.csv(file_path, header=True, inferSchema=True)
                    dt = self.create_counted_data(day_dt)
                    self.save_data(self.inter_dir, dt, day)

                    if data is None:
                        data = day_dt
                    else:
                        data = data.union(day_dt)
                else:
                    path = os.path.join(self.inter_dir, f'{day.strftime("%Y-%m-%d")}.csv')
                    existed_files.append(self.spark.read.csv(path, header=True, inferSchema=True))
        if data:
            data = self.create_counted_data(data)
            existed_files.append(data)
        end_date = self.merge_and_sum(existed_files)
        self.save_data(self.dir_name_out, end_date, self.date_all)

    def merge_and_sum(self, dfs):
        data = dfs[0]
        for df in dfs[1:]:
            data = data.union(df)
        result = data.groupBy("email").agg(
            F.sum("create_count").alias("create_count"),
            F.sum("read_count").alias("read_count"),
            F.sum("update_count").alias("update_count"),
            F.sum("delete_count").alias("delete_count")
        )
        return result

    def count_actions(self, data):
        actions = ['CREATE', 'READ', 'UPDATE', 'DELETE']
        counts = {}

        for action in actions:
            counts[f'{action.lower()}_count'] = self.day_count_one_act(data, action)

        result = data.groupBy("email").agg(
            *[F.sum(counts[action]).alias(f'{action}_count') for action in actions]
        )
        return result

    def day_count_one_act(self, data, action):
        return F.when(data['action'] == action, 1).otherwise(0)

    @staticmethod
    def run(date_all: str):
        date_all = datetime.strptime(date_all.strip(), "%Y-%m-%d")
        data_agreg = DataAggregator(date_all=date_all)
        data_agreg.create_data()