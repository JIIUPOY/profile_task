import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException

class DataAggregator:

    def __init__(self, date_all: datetime, days_count: int = 7, dir_name_in: str = 'input',
                 dir_name_out: str = 'output', temp_dir: str = 'temp'):
        self.date_all = date_all
        self.begin_date = date_all - timedelta(days= days_count)
        self.dir_name_in = dir_name_in
        self.dir_name_out = dir_name_out
        self.days_count = days_count
        self.temp_dir = temp_dir

    def check_files_existance(self, dir: str, day: datetime) -> bool:
        input_file_path = os.path.join(dir, f'{day.strftime("%Y-%m-%d")}.csv')
        return os.path.exists(input_file_path)

    def save_data(self, dir: str, dt: pd.DataFrame, day: datetime):
        output_path = os.path.join(dir, f'{day.strftime("%Y-%m-%d")}.csv')
        if not os.path.exists(dir):
            os.makedirs(dir)
        dt.to_csv(output_path, index=False)

    def create_counted_data(self, dt: pd.DataFrame) -> pd.DataFrame:
        data = self.count_actions(dt)
        return data

    def create_data(self):
        data = pd.DataFrame()
        existed_files = []
        for index in range(self.days_count):
            day = self.begin_date + timedelta(days=index)
            if not self.check_files_existance(self.dir_name_in, day):
                raise AirflowException("File not found!")
            else:
                if not self.check_files_existance(self.temp_dir, day):
                    file_path = os.path.join(self.dir_name_in, f'{day.strftime("%Y-%m-%d")}.csv')
                    day_dt = pd.read_csv(file_path, sep=',', names=['email', 'action', 'dt'])
                    dt = self.create_counted_data(day_dt)
                    self.save_data(self.temp_dir, dt, day)
                    data = pd.concat([data, day_dt], ignore_index=True)
                    data['dt'] = pd.to_datetime(data['dt']).dt.date
                    data = data.sort_values(by=['email', 'dt'])
                else:
                    path = os.path.join(self.temp_dir, f'{day.strftime("%Y-%m-%d")}.csv')
                    existed_files.append(pd.read_csv(path))
        if not data.empty:
            data = self.create_counted_data(data)
            existed_files.append(data)
            end_date = self.merge_sum_data(existed_files)
            self.save_data(self.dir_name_out, end_date, self.date_all)

        else:
            existed_files.append(data)
            end_date = self.merge_sum_data(existed_files)
            self.save_data(self.dir_name_out, end_date, self.date_all)

    def merge_sum_data(self, df: list[pd.DataFrame]) -> pd.DataFrame:
        cols = ['create_count', 'read_count', 'update_count', 'delete_count']
        data = pd.concat(df).groupby(["email"], as_index=False)[cols].sum()
        return data


    def count_actions(self, data: pd.DataFrame) -> pd.DataFrame:
        act_names = np.array(['CREATE', 'READ', 'UPDATE', 'DELETE'])
        act_numb = {}
        for item in act_names:
            act_numb[f'{item.lower()}_count'] = self.day_count_one_act(data, item)
        end_date = pd.DataFrame(act_numb).reset_index()
        return end_date

    def day_count_one_act(self, dt: pd.DataFrame, act: str) -> pd.Series:
        return dt[dt['action'] == act].groupby('email').size()