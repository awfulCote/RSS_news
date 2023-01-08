import os
import psycopg2
import feedparser
from datetime import datetime
from airflow.models import Variable

from source.download_data import get_conn_credentials

def create_source_table(**kwargs):
    source_name = kwargs['name']

    conn_id = Variable.get("conn_id")
    conn_to_airflow = get_conn_credentials(conn_id)

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port, \
        conn_to_airflow.login, conn_to_airflow.password, \
        conn_to_airflow.schema

    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    cursor = pg_conn.cursor()

    cursor.execute(f"CREATE TABLE IF NOT EXISTS {source_name}_news ("
                   f"NewsId INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
                   f"FileId INT NOT NULL,"
                   f"NewsCategory CHARACTER VARYING(50) NOT NULL,"
                   f"NewsTitle CHARACTER VARYING(500) NOT NULL,"
                   f"NewsGuid CHARACTER VARYING(300) NOT NULL,"
                   f"NewsDescription TEXT,"
                   f"NewsPubDate INT NOT NULL,"
                   f"CONSTRAINT fk_file "
                   f"FOREIGN KEY(FileId) "
                   f"REFERENCES rss_files(FileId) "
                   f"ON DELETE CASCADE) ")

    pg_conn.commit()

    cursor.close()
    pg_conn.close()

def fill_source_table(**kwargs):
    source_name = kwargs['name']

    conn_id = Variable.get("conn_id")
    conn_to_airflow = get_conn_credentials(conn_id)

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port, \
        conn_to_airflow.login, conn_to_airflow.password, \
        conn_to_airflow.schema

    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    cursor = pg_conn.cursor()

    #Getting last news's date in source table
    cursor.execute(f"select MAX(tn.newspubdate) from {source_name}_news tn")

    date_of_last_record = cursor.fetchall()
    date_of_last_record = date_of_last_record[0][0]

    if not date_of_last_record:
        date_of_last_record = 0

    #Getting the last table with news's files
    cursor.execute(f"SELECT rf.fileid, rf.filename, rf.sourcename, rf.requestdate FROM rss_files rf "
                   f"INNER JOIN"
                   f"(SELECT fc.fileid "
                   f"FROM files_checks fc "
                   f"WHERE fc.fileexist = true "
                   f"AND fc.filenotempty = true "
                   f"AND fc.fileformat = true) ids "
                   f"ON rf.fileid = ids.fileid "
                   f"WHERE rf.sourcename='{source_name}' AND rf.requestdate>{date_of_last_record} "
                   f"ORDER BY rf.fileid ")

    file_records = cursor.fetchall()

    for record in file_records:

        items_for_table = {'published_parsed': 0, 'tags': '', 'title': '', 'summary': '', 'link': ''}

        fileid, filename, sourcename, requestdate = record

        if requestdate > date_of_last_record:

            #Filling the dictionary with publication data
            path_to_file = os.path.join('/opt/airflow/files', filename)
            feed = feedparser.parse(path_to_file)
            feed = feed.entries
            for entry in feed[::-1]:
                keys = [i for i in items_for_table.keys() if i in entry.keys()]
                for key in keys:
                    items_for_table[key] = entry[key]
                    if key == 'tags':
                        items_for_table[key] = entry[key][0]['term']

                pub_date = int(datetime(*items_for_table['published_parsed'][:6]).timestamp())

                # Check for actuality
                if pub_date > date_of_last_record:
                    # Record news to table
                    cursor.execute(
                        f"INSERT INTO {source_name}_news (FileId, NewsCategory, NewsTitle, NewsGuid, NewsDescription, NewsPubDate) "
                        f"VALUES (%s, %s, %s, %s, %s, %s)",
                        (fileid, items_for_table['tags'], items_for_table['title'], items_for_table['link'],
                         items_for_table['summary'], pub_date))

                    date_of_last_record = pub_date

        pg_conn.commit()

    cursor.close()
    pg_conn.close()

def create_category_table(**kwargs):

    source_name = kwargs['name']

    conn_id = Variable.get("conn_id")
    conn_to_airflow = get_conn_credentials(conn_id)

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port, \
        conn_to_airflow.login, conn_to_airflow.password, \
        conn_to_airflow.schema

    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    cursor = pg_conn.cursor()

    # Getting the list of categories for source
    cursor.execute(f"SELECT NewsCategory from {source_name}_news "
                   f"GROUP BY NewsCategory ")

    source_categories = cursor.fetchall()

    # Getting the list of categories
    cursor.execute("SELECT * from news_categories")


    # Creating the categories table for the source
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {source_name}_categories ("
                   f"{source_name}CategoryId INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
                   f"CategoryId INT NOT NULL,"
                   f"CategoryName CHARACTER VARYING(50) NOT NULL UNIQUE, "
                   f"CONSTRAINT fk_category "
                   f"FOREIGN KEY(CategoryId) "
                   f"REFERENCES news_categories(CategoryId) "
                   f"ON DELETE CASCADE)")
                   
    # Adding the new category to table
    for source_category in source_categories:
        cursor.execute(
            f"INSERT INTO {source_name}_categories (CategoryName) VALUES (%s) ON CONFLICT DO NOTHING",
            ( source_category))

    pg_conn.commit()

    cursor.close()
    pg_conn.close()
