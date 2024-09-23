import os
import pandas as pd
import sys
from datetime import datetime, timedelta
from typing import Union

def act_count(dt: pd.DataFrame, act: str) -> pd.Series:
      return dt[dt['action'] == act].groupby('email').size()

d: int = 7
date_all: datetime = datetime.strptime(str(sys.argv[1]), '%Y-%m-%d').date()
b_date: datetime = date_all - timedelta(days= d)

dir_name_out: str = 'output'
dir_name_in: str = 'input'

data: pd.DataFrame = pd.DataFrame()
for index in range(d):
      dt = pd.read_csv(#добавить проверку, что есть файлы до определенной даты, иначе выводить ошибку с предложением на вывод минимальной даты
            os.path.join(dir_name_in, f'{(b_date + timedelta(days= index))}.csv'),
            sep=',', names=['email', 'action', 'dt'])
      data = pd.concat([data, dt])


data['dt']: pd.Series = pd.to_datetime(data['dt']).dt.date
data: pd.DataFrame = data.sort_values(by=['email', 'dt'])

create_count: pd.Series = act_count(data, 'CREATE')
read_count: pd.Series = act_count(data, 'READ')
update_count: pd.Series = act_count(data, 'UPDATE')
delete_count: pd.Series = act_count(data, 'DELETE')

end_data: pd.DataFrame = pd.DataFrame({
      'create_count' : create_count,
      'read_count' : read_count,
      'update_count' : update_count,
      'delete_count' : delete_count
}).reset_index()


if not os.path.exists(dir_name_out):
      os.makedirs(dir_name_out)

path: str = os.path.join(dir_name_out, f'{date_all}.csv')
end_data.to_csv(path, index= False)


if __name__ == "__main__":
      print(1)