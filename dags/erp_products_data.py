from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from config.erp_products_data_config import default_args
from utils.utils import truncate_table
import os
import pymssql
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

# Load environment variables from .env file
load_dotenv()
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')
BYOD_SERVER = os.getenv('BYOD_SERVER')
BYOD_DATABASE = 'PRODUCTS'
BYOD_USERNAME = os.getenv('BYOD_USERNAME')
BYOD_PASSWORD = os.getenv('BYOD_PASSWORD')

# DAG definition
dag = DAG(
    'erp_products_data',
    default_args=default_args,
    description='DAG to extract product data from ERP and consolidate'
    'it in a table in Snowflake',
    schedule_interval='@weekly',
    catchup=False,
)


def get_products_data(cursor, table_name, columns):
    '''
    Retrieves product data from the ERP.

    Parameters:
    - cursor: Cursor for SQL queries execution.
    - table_name (str): Table name for data extraction.
    - columns (str): Columns to select in SQL query.

    Returns:
    - pandas.DataFrame: DataFrame containing the product data.
    '''
    query = f'SELECT {columns} FROM {table_name}'
    print('[BYOD] Executing query')
    cursor.execute(query)
    print('[BYOD] Query finished')
    result = cursor.fetchall()
    return pd.DataFrame(result) if result else None


def process_data(df):
    '''
    Processes the DataFrame by converting data types and handling missing
    values.

    Parameters:
    - df (pandas.DataFrame): DataFrame containing the product data.

    Returns:
    - pandas.DataFrame: The processed DataFrame ready for further operations.
    '''
    print('[AIRFLOW] Dataframe processing started')

    # Convert COSTO_HISTORICO to numeric, errors to NaN, then fill NaN with 0
    df['COSTO_HISTORICO'] = \
        pd.to_numeric(df['COSTO_HISTORICO'], errors='coerce').fillna(0)

    # Ensure the column is of type float
    df['COSTO_HISTORICO'] = df['COSTO_HISTORICO'].astype(float)

    # Fill NaN values in other columns with empty strings
    string_columns = df.columns.drop('COSTO_HISTORICO')
    df[string_columns] = df[string_columns].fillna('')

    print('[AIRFLOW] Dataframe processed')
    print(df.head())
    print(df.shape)
    return df


def get_products_data_and_load():
    '''
    Extracts product data from the ERP, processes it, and
    loads it into Snowflake.

    Fetches all data at once and writes it in one operation.
    '''
    print('[Start execution] Get ERP product data')

    # Get Snowflake connection
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn_snowflake = hook.get_conn()
    cursor_snowflake = conn_snowflake.cursor()

    # Truncate table before loading data
    truncate_table('ERP_PRODUCTS', SNOWFLAKE_CONN_ID)

    # Connect to ERP database
    conn = pymssql.connect(
        BYOD_SERVER, BYOD_USERNAME, BYOD_PASSWORD, BYOD_DATABASE
    )
    table_name = default_args['byod_erp_products_table_name']
    columns = ', '.join([
        column[0]
        for column in default_args['snowflake_erp_products_table_columns']
    ])
    cursor = conn.cursor(as_dict=True)
    df = get_products_data(cursor, table_name, columns)
    if df is not None and not df.empty:
        processed_df = process_data(df)
        print('[SNOWFLAKE] Write data')

        # Write data to Snowflake
        success, nchunks, nrows, _ = write_pandas(
            conn_snowflake, processed_df, 'ERP_PRODUCTS'
        )
        if not success:
            raise Exception('Failed to write data to Snowflake')
        print(f'Successfully wrote {nrows} rows to ERP_PRODUCTS in Snowflake.')
    else:
        print('[BYOD] Query result empty!')
    conn.close()
    cursor_snowflake.close()
    conn_snowflake.close()


def run_get_products_data():
    '''
    Executes the task to get products data and load into Snowflake.
    '''
    get_products_data_and_load()


task_get_products_data = PythonOperator(
    task_id='get_products_data',
    python_callable=run_get_products_data,
    dag=dag,
)
