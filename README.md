## Анализ публикуемых новостей

### Описание
Создание ETL-процесс формирования витрин данных для анализа публикаций новостей из RSS рассылок.

В этой реализации сформирована витрина со следующей информацией:
- Суррогатный ключ категории;
- Название категории;
- Общее количество новостей из всех источников по данной категории за все время;
- Количество новостей данной категории для каждого из источников за все время;
- Общее количество новостей из всех источников по данной категории за последние сутки;
- Количество новостей данной категории для каждого из источников за последние сутки;
- Среднее количество публикаций по данной категории в сутки;
- День, в который было сделано максимальное количество публикаций по данной категории;
- Количество публикаций новостей данной категории по дням недели.


В качестве оркестратора используется [Airflow](https://airflow.apache.org/). 
Скрипты написаны на Python, запросы - PostgreSQL.

### Установка

Установка реализована через docker-compose (https://docs.docker.com/compose/install/).
Для GUI интерфейса можно также использовать docker(https://docs.docker.com/engine/install/) .

Запуск происходит из корневой папки проекта поднятием docker-compose:
``` bash
docker-compose up -d
```

### Настройка Airflow

GUI Airflow:
http://localhost:8080/

Необходимо настроить соединение с БД, добавить словарь с источниками RSS файлов.

Данные для подключения:
user: airflow
password: airflow


#### Настройка соединения с БД

БД [PostgreSQL](https://www.postgresql.org/).

Данные для подключения:
user: admin
password: admin
db: news


В Airflow (раздел *Connections*) добавить соединение:
Connection Id:  postgre_conn
Connection Type: Postgres
Host: host.docker.internal
Schema: news
Login: admin
Password: admin
Port: 5430


В разделе *Variables* добавить переменную:
Key: conn_id
Val: postgre_conn


#### Для добавления словаря с источниками RSS

В разделе *Variables* добавить переменную:
Key: urls_dict
Val: {"lenta": "https://lenta.ru/rss/", "vedomosti":"https://www.vedomosti.ru/rss/news", "tass":"https://tass.ru/rss/v2.xml"}
Description: Dictionary of urls   


### Подключение к БД через GUI

Подключение через pgAdmin4 [pgAdmin](https://www.pgadmin.org/).

http://localhost:5050/


Данные для подключения:
user: pgadmin
password: pgadmin
