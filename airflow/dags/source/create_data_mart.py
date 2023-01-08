import psycopg2
import datetime
import operator
import functools
import collections
import numpy as np

from airflow.models import Variable

from source.download_data import get_conn_credentials

def create_data_mart():

    # Getting the data sources
    urls_dict = eval(Variable.get("urls_dict"))

    conn_id = Variable.get("conn_id")
    conn_to_airflow = get_conn_credentials(conn_id)

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port, \
        conn_to_airflow.login, conn_to_airflow.password, \
        conn_to_airflow.schema

    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    cursor = pg_conn.cursor()

    cursor.execute(f"CREATE TABLE IF NOT EXISTS data_mart ("
                   f"DMCId INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
                   f"DMCCategoryId INT NOT NULL UNIQUE,"
                   f"DMCName CHARACTER VARYING(50) NOT NULL,"
                   f"DMCNewsTotalCount INT NOT NULL,"
                   f"DMCNewsDailyCount INT NOT NULL,"
                   f"DMCMeanDailyCount FLOAT NOT NULL,"
                   f"DMCMaxCountDate DATE NOT NULL,"
                   f"DMCMondayCount INT NOT NULL,"
                   f"DMCTuesdayCount INT NOT NULL,"
                   f"DMCWednesdayCount INT NOT NULL,"
                   f"DMCThursdayCount INT NOT NULL,"
                   f"DMCFridayCount INT NOT NULL,"
                   f"DMCSaturdayCount INT NOT NULL,"
                   f"DMCSundayCount INT NOT NULL) ")

    for name in urls_dict.keys():
        cursor.execute(f"ALTER TABLE data_mart ADD COLUMN IF NOT EXISTS DMC{name}NewsTotalCount INTEGER")
        cursor.execute(f"ALTER TABLE data_mart ADD COLUMN IF NOT EXISTS DMC{name}NewsDailyCount INTEGER")

    pg_conn.commit()

    cursor.close()
    pg_conn.close()

def fill_data_mart():

    # Getting the data sources
    urls_dict = eval(Variable.get("urls_dict"))

    conn_id = Variable.get("conn_id")
    conn_to_airflow = get_conn_credentials(conn_id)

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port, \
        conn_to_airflow.login, conn_to_airflow.password, \
        conn_to_airflow.schema

    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    cursor = pg_conn.cursor()

    # Getting the categories
    cursor.execute("SELECT * from news_categories")
    categories = cursor.fetchall()

    query_data = dict()
    dow_dict = {1: 'DMCMondayCount', 2: 'DMCTuesdayCount', 3: 'DMCWednesdayCount', 4: 'DMCThursdayCount',
                5: 'DMCFridayCount', 6: 'DMCSaturdayCount', 7: 'DMCSundayCount'}

    for category_id, category in categories:

        query_data[category_id] = dict()
        query_data[category_id]['DMCCategoryId'] = category_id
        query_data[category_id]['DMCName'] = f"'{category}'"

        date_count_dict = dict()
        category_count_dict = dict()

        total_count = 0
        daily_count = 0
        daily_mean_count = []

        # The cycle through the all data sources 
        for name in urls_dict.keys():

            # The total number of news from all sources in the category for all time
            cursor.execute(f"SELECT COUNT(*) FROM {name}_news nws "
                           f"WHERE nws.newscategory IN "
                           f"( SELECT ct.categoryname FROM {name}_categories ct "
                           f"WHERE ct.categoryid = {category_id} )")

            source_total_count = cursor.fetchall()[0][0]
            query_data[category_id][f'DMC{name.capitalize()}NewsTotalCount'] = source_total_count
            total_count += source_total_count

            # The total number of news from all sources in the category for the last day
            cursor.execute(f"SELECT COUNT(*) FROM {name}_news nws "
                           f"WHERE nws.newscategory IN "
                           f"( SELECT ct.categoryname FROM {name}_categories ct "
                           f"WHERE ct.categoryid = {category_id} ) "
                           f"AND to_timestamp(nws.newspubdate)::timestamptz at time zone 'UTC' "
                           f"BETWEEN CURRENT_TIMESTAMP - INTERVAL '1 day' AND CURRENT_TIMESTAMP")

            source_daily_count = cursor.fetchall()[0][0]
            query_data[category_id][f'DMC{name.capitalize()}NewsDailyCount'] = source_daily_count
            daily_count += source_daily_count

            # Average number of publications in the category per day
            cursor.execute(f"SELECT ROUND(AVG(cnt),2) FROM ( "
                           f"SELECT DATE_TRUNC('day', to_timestamp(nws.newspubdate)::timestamptz at time zone 'UTC') AS day, COUNT(*) AS cnt "
                           f"FROM {name}_news nws "
                           f"WHERE nws.newscategory "
                           f"IN ( select ct.categoryname FROM {name}_categories ct WHERE ct.categoryid = {category_id} ) "
                           f"GROUP BY DATE_TRUNC('day', to_timestamp(nws.newspubdate)::timestamptz AT time zone 'UTC') ) dc ")

            source_daily_mean_count = cursor.fetchall()[0][0]
            if not source_daily_mean_count:
                source_daily_mean_count = 0
            daily_mean_count.append(float(source_daily_mean_count))

            # The day on which the maximum number of publications in the category was made
            cursor.execute(
                f"SELECT DATE_TRUNC('day', to_timestamp(nws.newspubdate)::timestamptz at time zone 'UTC') AS day, COUNT(*) AS cnt "
                f"FROM {name}_news nws "
                f"WHERE nws.newscategory "
                f"IN ( select ct.categoryname FROM {name}_categories ct WHERE ct.categoryid = {category_id} ) "
                f"GROUP BY DATE_TRUNC('day', to_timestamp(nws.newspubdate)::timestamptz AT time zone 'UTC')")

            date_count_dict[name] = {int(datetime.datetime.timestamp(x[0])): x[1] for x in cursor.fetchall()}

            # The number of news publications in the category by day of the week
            cursor.execute(
                f"SELECT EXTRACT('isodow' FROM to_timestamp(nws.newspubdate)::timestamptz AT time zone 'UTC') AS isodow, COUNT(*) AS cnt "
                f"FROM {name}_news nws WHERE nws.newscategory "
                f"IN ( select ct.categoryname FROM {name}_categories ct WHERE ct.categoryid = {category_id} ) "
                f"GROUP BY isodow")

            category_count_dict[name] = {int(x[0]): x[1] for x in cursor.fetchall()}

        query_data[category_id]['DMCNewsTotalCount'] = total_count
        query_data[category_id]['DMCNewsDailyCount'] = daily_count
        query_data[category_id]['DMCMeanDailyCount'] = np.round(np.mean(daily_mean_count), 2)

        date_count_dict = dict(functools.reduce(operator.add, map(collections.Counter, date_count_dict.values())))
        query_data[category_id][
            'DMCMaxCountDate'] = f"'{datetime.datetime.fromtimestamp(max(date_count_dict, key=date_count_dict.get))}'"

        category_count_dict = dict(
            functools.reduce(operator.add, map(collections.Counter, category_count_dict.values())))
        for number, name in dow_dict.items():
            try:
                query_data[category_id][name] = category_count_dict[number]
            except:
                query_data[category_id][name] = 0

    # Cleaning the data mart
    cursor.execute("TRUNCATE TABLE data_mart")

    # Filling the data mart
    for _, query in query_data.items():
        cursor.execute(
            "INSERT INTO data_mart (" + ("{}, " * len(query.keys())).format(*query.keys())[:-2] +
            ") VALUES (" + ("{}, " * len(query.values())).format(*query.values())[:-2] + ") ")

    pg_conn.commit()

    cursor.close()
    pg_conn.close()
