import os
import pandas as pd
import sys
import numpy as np
from datetime import datetime, timedelta

class DataAgregation:

    def __init__(self, date_all: datetime, d: int = 7, dir_name_in: str = 'input',
                 dir_name_out: str = 'output', inter_dir: str = 'inter'):
        self.date_all = date_all
        self.begin_date = date_all - timedelta(days= d)
        self.dir_name_in = dir_name_in
        self.dir_name_out = dir_name_out
        self.d = d
        self.inter_dir = inter_dir

    def check_files_exist(self, dir: str, day: datetime) -> bool:
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
        for index in range(self.d):
            day = self.begin_date + timedelta(days=index)
            if not self.check_files_exist(self.dir_name_in, day):
                print(f"File for {day.strftime('%Y-%m-%d')} does not exist!")
                sys.exit(1)
            else:
                if not self.check_files_exist(self.inter_dir, day):
                    file_path = os.path.join(self.dir_name_in, f'{day.strftime("%Y-%m-%d")}.csv')
                    day_dt = pd.read_csv(file_path, sep=',', names=['email', 'action', 'dt'])
                    dt = self.create_counted_data(day_dt)
                    self.save_data(self.inter_dir, dt, day)
                    data = pd.concat([data, day_dt], ignore_index=True)
                    data['dt'] = pd.to_datetime(data['dt']).dt.date
                    data = data.sort_values(by=['email', 'dt'])
                else:
                    path = os.path.join(self.inter_dir, f'{day.strftime("%Y-%m-%d")}.csv')
                    existed_files.append(pd.read_csv(path))
        if not data.empty:
            data = self.create_counted_data(data)
            existed_files.append(data)
            end_date = self.merge_and_sum(existed_files)
            self.save_data(self.dir_name_out, end_date, self.date_all)

        else:
            existed_files.append(data)
            end_date = self.merge_and_sum(existed_files)
            self.save_data(self.dir_name_out, end_date, self.date_all)

    def merge_and_sum(self, df: list[pd.DataFrame]) -> pd.DataFrame:
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

    @staticmethod
    def run(date_all: str):
        date_all = datetime.strptime(date_all, "%Y-%m-%d")
        data_agreg = DataAgregation(date_all=date_all)
        data_agreg.create_data()

if __name__ == "__main__":
    DataAgregation.run()