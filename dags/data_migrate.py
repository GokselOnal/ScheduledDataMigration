from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow import DAG
import logging
import csv


def get_last_row(ti):
    hook = PostgresHook(postgres_conn_id="datawarehouse_46.101.167.19")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(CUSTOMER_ID) FROM data_destination LIMIT 10"),
    data = cursor.fetchone()
    if data[0] == None: ti.xcom_push(key="last_id", value=-1)
    else: ti.xcom_push(key="last_id", value=data[0])
    cursor.close()
    conn.close()


def get_data(ti):
    hook = PostgresHook(postgres_conn_id="datawarehouse_46.101.167.19")
    conn = hook.get_conn()
    cursor = conn.cursor()
    last_row = ti.xcom_pull(task_ids='LAST_ROW_ID', key='last_id')
    sql = f"SELECT * FROM data WHERE CUSTOMER_ID > {last_row} LIMIT 50"
    cursor.execute(sql)
    with open(f"dags/source_data.csv", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
        f.flush()
        cursor.close()
        conn.close()
        logging.info("Saved orders data in csv file: %s", f"dags/source_data.csv")


default_args = {
    'owner': 'GokselOnal',
    "email": ["can.onal@ozu.edu.tr"],
    "email_on_failure": True,
    "email_on_retry": False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


dag = DAG(dag_id="ETL",
          default_args=default_args,
          start_date=datetime(2022, 10, 18),
          schedule_interval='0 0 * * *'
          )

task1 = PostgresOperator(
    task_id="CREATE_TABLE",
    postgres_conn_id="datawarehouse_46.101.167.19",
    sql = """
        CREATE TABLE IF NOT EXISTS data_destination(
            CUSTOMER_ID INTEGER,
            GENRE VARCHAR(6),
            AGE INTEGER,
            ANNUAL_INCOME INTEGER,  
            SPENDING_SCORE INTEGER, 
            CITY_NAME VARCHAR(21), 
            CITY_LATITUDE DECIMAL(8,6),
            CITY_LONGITUDE DECIMAL(9,6),
            COUNTRY_CODE VARCHAR(3),
            CITY_POPULATION INTEGER,  
            IS_CAPITAL BOOL,
            PRODUCT_NAME VARCHAR(110), 
            PRODUCT_BRAND VARCHAR(20), 
            PRODUCT_RATING INTEGER,
            PRODUCT_PRICE FLOAT,
            COUNTRY_NAME VARCHAR(20),
            DATE DATE
        )
    """,
    dag = dag
)
task2 = PythonOperator(
    task_id = "LAST_ROW_ID",
    python_callable = get_last_row,
    dag = dag
)

task3 = PythonOperator(
    task_id = "GET_SOURCE_DATA",
    python_callable = get_data,
    dag = dag
)

task4 = BashOperator(
    task_id = "LOAD_DATA",
    bash_command = """ psql -h 46.101.167.19 -U postgres -p 5432 datawarehouse -c "\copy data_destination from '/opt/airflow/dags/source_data.csv' delimiter ',' csv header;" """,
    dag = dag
)

task1 >> task2 >> task3 >> task4
