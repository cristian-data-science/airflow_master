from datetime import timedelta, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from dags.config.erp_customer_data_config import default_args
from dags.utils.utils import write_data_to_snowflake
import os
import pymssql
import pandas as pd


# Load environment variables from .env file
load_dotenv()
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')
BYOD_SERVER = os.getenv('BYOD_SERVER')
BYOD_DATABASE = os.getenv('BYOD_DATABASE')
BYOD_USERNAME = os.getenv('BYOD_USERNAME')
BYOD_PASSWORD = os.getenv('BYOD_PASSWORD')

# Dag definition
dag = DAG(
    'erp_customer_data',
    default_args=default_args,
    description='DAG to extract customer data from ERP and '
    'consolidate it in a single table in Snowflake',
    schedule_interval='0 5 * * *',
)


# Tasks functions
def get_byod_customers(days=0):
    '''
    Retrieves BYOD customer data for a specified number of days.

    Connects to a database using provided credentials and retrieves customer
    data from a defined table and columns, filtered based on the
    SYNCSTARTDATETIME to get records from 'days' ago to today. Converts the
    result into a pandas DataFrame.

    Parameters:
    days (int): Number of days to look back from today. Default is 0 (today).

    Returns:
    pd.DataFrame: DataFrame with customer data. Empty if no data found,
                  None if no query result.

    Note
    Uses global variables for database settings and table/column names.
    Ensure these are correctly set.
    '''

    today = date.today()
    start_date = today - timedelta(days=days)
    print(f'[Start execution] Get byod customers from {start_date} to {today}')
    conn = pymssql.connect(
        BYOD_SERVER, BYOD_USERNAME, BYOD_PASSWORD, BYOD_DATABASE
    )
    cursor = conn.cursor(as_dict=True)
    table_name = default_args['byod_erp_customer_table_name']
    columns = ', '.join([
        column[0]
        for column in default_args['byod_erp_customer_table_interest_columns']
    ])
    query = f'SELECT {columns} FROM {table_name}'
    if days is not None:
        query += f'''
        WHERE CAST(SYNCSTARTDATETIME AS DATE)
        BETWEEN
            CAST(DATEADD(DAY, -{days}, GETDATE()) AS DATE)
            AND CAST(GETDATE() AS DATE)'''
    cursor.execute(query)
    result = cursor.fetchall()
    if not result:
        return None
    df_byod_customers = pd.DataFrame(result)
    if df_byod_customers.empty:
        return df_byod_customers
    df_byod_customers['SYNCSTARTDATETIME'] = \
        df_byod_customers['SYNCSTARTDATETIME'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_byod_customers['ADDRESSVALIDFROM'] = \
        df_byod_customers['ADDRESSVALIDFROM'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_byod_customers['DELIVERYADDRESSVALIDFROM'] = \
        df_byod_customers[
            'DELIVERYADDRESSVALIDFROM'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_byod_customers['CREDMANCUSTOMERSINCE'] = \
        df_byod_customers[
            'CREDMANCUSTOMERSINCE'].dt.strftime('%Y-%m-%d %H:%M:%S')
    print(df_byod_customers.head())
    print(df_byod_customers.shape)
    return df_byod_customers


def create_snowflake_temporary_table(cursor, temp_table_name, columns):
    '''
    Creates a temporary table in Snowflake with a defined column structure.

    This function is useful for preparing the Snowflake environment for data
    insertion or update operations.

    Parameters:
    - cursor: A database cursor for Snowflake to execute SQL commands.
    - temp_table_name (str): The name of the temporary table to be created.

    The function uses the provided cursor to execute an SQL command that
    creates a temporary table in Snowflake. The table structure is defined
    based on the columns specified in columns.
    '''
    create_temp_table_sql = f'CREATE TEMPORARY TABLE {temp_table_name} ('
    create_temp_table_sql += \
        ', '.join([f'{name} {type}' for name, type in columns]) + ');'

    print(create_temp_table_sql)
    cursor.execute(create_temp_table_sql)


def run_get_byod_customers(**context):
    execution_date = context['execution_date']
    print(f'Execution Date: {execution_date}')
    df_byod_customers = get_byod_customers(days=3)
    write_data_to_snowflake(
        df_byod_customers,
        'ERP_CUSTOMERS',
        default_args['byod_erp_customer_table_interest_columns'],
        ['CUSTOMERACCOUNT'],
        'TEMP_ERP_CUSTOMERS',
        SNOWFLAKE_CONN_ID
    )


# Task definitions
task_1 = PythonOperator(
    task_id='get_byod_customers',
    python_callable=run_get_byod_customers,
    dag=dag,
)
