from datetime import datetime

from source.download_data import download_all
from source.create_data_mart import create_data_mart, fill_data_mart
from source.create_tables import create_source_table, fill_source_table, create_category_table
from source.check_data import check_file_is_not_empty

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator


# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="news_run", start_date=datetime(2022, 12, 31), catchup=False,
         schedule_interval="0 0 * * *") as dag:

    # Create the dictionary for loading sources
    urls_dict = eval(Variable.get("urls_dict"))

    # Download data from sources and create the tables
    download_tasks = []
    create_tables_tasks = []
    fill_tables_tasks = []
    create_categories_tables_tasks = []

    for name, url in urls_dict.items():
        download_tasks.append(
            PythonOperator(task_id=f"download_news_{name}", python_callable=download_all,
                                             op_kwargs={'url': url, 'name': name}, do_xcom_push=False))
        create_tables_tasks.append(
            PythonOperator(task_id=f"create_table_{name}", python_callable=create_source_table,
                           op_kwargs={'name': name}, do_xcom_push=False))
        fill_tables_tasks.append(
            PythonOperator(task_id=f"fill_table_{name}", python_callable=fill_source_table,
                           op_kwargs={'name': name}, do_xcom_push=False))
        create_categories_tables_tasks.append(
            PythonOperator(task_id=f"create_categories_table_{name}", python_callable=create_category_table,
                           op_kwargs={'name': name}, do_xcom_push=False))

    # Checking files
    check_empty_task = PythonOperator(task_id="check_not_empty", python_callable=check_file_is_not_empty, do_xcom_push=False)

    # Creation and filling the data mart
    create_data_mart_task = PythonOperator(task_id="create_data_marts", python_callable=create_data_mart, do_xcom_push=False)
    fill_data_mart_task = PythonOperator(task_id="fill_data_marts", python_callable=fill_data_mart, do_xcom_push=False)

    # Set dependencies between tasks
    for task_1,task_2,task_3 in zip(create_tables_tasks, fill_tables_tasks, create_categories_tables_tasks):
        task_1.set_downstream(task_2)
        task_2.set_downstream(task_3)
        task_3.set_downstream(create_data_mart_task)

    #Tasks for creation the data
    download_tasks >> check_empty_task 
    #Tasks for creation the data mart
    create_data_mart_task >> fill_data_mart_task