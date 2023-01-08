import os
import psycopg2
from airflow.models import Variable

from source.download_data import get_conn_credentials


def check_file_is_not_empty():

    conn_id = Variable.get("conn_id")
    conn_to_airflow = get_conn_credentials(conn_id)
    field = "fc.FileNotEmpty"

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port, \
        conn_to_airflow.login, conn_to_airflow.password, \
        conn_to_airflow.schema

    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    cursor = pg_conn.cursor()

    cursor.execute(f"SELECT rf.fileid, rf.filename FROM rss_files rf "
                   f"LEFT JOIN files_checks fc "
                   f"ON rf.fileid=fc.fileid "
                   f"WHERE {field} IS FALSE")

    file_records = cursor.fetchall()

    for record in file_records:
        fileid, filename = record
        path_to_file = os.path.join('/opt/airflow/files', filename)
        file_is_not_empty = os.stat(path_to_file).st_size != 0

        cursor.execute(f"UPDATE files_checks "
                       f"SET FileNotEmpty = {file_is_not_empty} "
                       f"WHERE FileId = {fileid}")

        if not file_is_not_empty:
            print(f"File {filename} is empty.")

    pg_conn.commit()

    cursor.close()
    pg_conn.close()

