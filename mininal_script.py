import os
import pandas as pd
import sys
from datetime import datetime, timedelta

def act_count(dt, act):
      return dt[dt['action'] == act].groupby('email').size()

d = 7
date_all = datetime.strptime(str(sys.argv[1]), '%Y-%m-%d').date()
b_date = date_all - timedelta(days= d)

end_cols = ['email', 'create_count', 'read_count', 'update_count', 'delete_count']


dir_name_out = 'output'
dir_name_in = 'input'
data = pd.DataFrame()
for index in range(d):
      dt = pd.read_csv(
            os.path.join(dir_name_in, f'{(b_date + timedelta(days= index))}.csv'),
            sep=',', names=['email', 'action', 'dt'])
      data = pd.concat([data, dt])


data['dt'] = pd.to_datetime(data['dt']).dt.date
data = data.sort_values(by=['email', 'dt'])

create_count = act_count(data, 'CREATE')
read_count = act_count(data, 'READ')
update_count = act_count(data, 'UPDATE')
delete_count = act_count(data, 'DELETE')

end_data = pd.DataFrame({
      'create_count' : create_count,
      'read_count' : read_count,
      'update_count' : update_count,
      'delete_count' : delete_count
}).reset_index()


if not os.path.exists(dir_name_out):
      os.makedirs(dir_name_out)

path = os.path.join(dir_name_out, f'{date_all}.csv')
end_data.to_csv(path, index= False)