# profile_task

## Настройка среды

### Требованием для запуска приложения является наличие Docker

### Запуск приложения

```rb
git clone https://github.com/JIIUPOY/profile_task.git
cd profile_task
make build
make up
```

### После чего перейдя по ссылке localhost:8080 будет доступен интерфейс для Airflow Apache.
Чтобы авторизироваться необходимо ввести логин и пароль - airflow.


### Методы класса DataAggregator:

***check_files_existance(self, dir: str, day: datetime)*** - проверят наличие файла в директории.

***save_data(self, dir: str, dt: pd.DataFrame, day: datetime)*** - сохраняет файл в выбранной директории.

***count_actions(self, data: pd.DataFrame)*** - создает DataFrame, в котором посчитаны все агрегированные действия для каждого уникального email и сгруппированы по нему.

***day_count_one_act(self, dt: pd.DataFrame, act: str)*** - возвращает Series, в котором для каждого уникального email посчитано количество определенного действия за один день.

***merge_sum_data(self, df: list[pd.DataFrame])*** - объединяет и суммирует данные из нескольких DataFrame по email.

***create_data(self)*** - создает итоговый агрегированный файл в директории output.


