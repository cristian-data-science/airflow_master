from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
import os
import pyodbc
import pandas as pd

# Cargamos las variables de entorno
load_dotenv()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1),
    'email': ['your-email@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sql_to_excel',
    default_args=default_args,
    description='DAG to connect to SQL Server and convert a table to Excel',
    schedule_interval=timedelta(days=1),
)

def connect_and_transform_data():
    # Crea la conexión
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DB")
    username = os.getenv("SQL_USER")
    password = os.getenv("SQL_PASS")
    table_name = os.getenv("SQL_TABLE")

    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                      server+';DATABASE='+database+';UID='+username+';PWD='+ password)

    # Lee la tabla y la convierte a un DataFrame
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, cnxn)

    # Escribe el DataFrame a un archivo Excel
    df.to_excel("output.xlsx", index=False)

t1 = PythonOperator(
    task_id='connect_and_transform',
    python_callable=connect_and_transform_data,
    dag=dag,
)
