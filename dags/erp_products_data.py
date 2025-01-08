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


def get_products_data_batch(cursor, table_name, columns, offset, batch_size):
    '''
    Retrieves a batch of product data from the ERP.

    Parameters:
    - cursor: Cursor for SQL queries execution.
    - table_name (str): Table name for data extraction.
    - columns (str): Columns to select in SQL query.
    - offset (int): Offset for batch processing.
    - batch_size (int): Number of records to fetch per batch.

    Returns:
    - pandas.DataFrame: DataFrame containing the batch data.
    '''
    query = f'''
        SELECT {columns}
        FROM {table_name}
        ORDER BY 1  -- Ensure consistent ordering
        OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY
    '''
    print(f'[BYOD] Executing query for batch starting at offset {offset}')
    cursor.execute(query)
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
    Extracts product data from the ERP in batches, processes it,
    and loads it into Snowflake.
    '''
    print('[Start execution] Get ERP product data')

    # Get Snowflake connection
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn_snowflake = hook.get_conn()
    cursor_snowflake = conn_snowflake.cursor()

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

    # Batch processing variables
    batch_size = 20000
    offset = 0
    batch_number = 1

    # Create a temporary table in Snowflake
    print('[SNOWFLAKE] Creating temporary table')
    cursor_snowflake.execute(
        '''CREATE TEMPORARY TABLE ERP_PRODUCTS_TEMP AS
        SELECT * FROM ERP_PRODUCTS WHERE 1=0
    ''')  # Structure only, no data

    try:
        while True:
            # Fetch batch from BYOD
            df = get_products_data_batch(
                cursor, table_name, columns, offset, batch_size)
            if df is None or df.empty:
                break

            # Process batch
            processed_df = process_data(df)

            # Write batch to Snowflake temporary table
            print(f'[SNOWFLAKE] Writing batch {batch_number} to'
                  ' temporary table')
            success, nchunks, nrows, _ = write_pandas(
                conn_snowflake, processed_df, 'ERP_PRODUCTS_TEMP'
            )
            if not success:
                raise Exception(f'Failed to write batch {batch_number} '
                                'to Snowflake')

            print(f'Successfully wrote {nrows} rows for batch {batch_number}')
            batch_number += 1
            offset += batch_size

        # If all batches are successfully written, move data to final table
        print('[SNOWFLAKE] Moving data from temporary table to final table')
        truncate_table('ERP_PRODUCTS', SNOWFLAKE_CONN_ID)
        cursor_snowflake.execute(
            'INSERT INTO ERP_PRODUCTS SELECT * FROM ERP_PRODUCTS_TEMP')

        print('Data successfully loaded into ERP_PRODUCTS in Snowflake.')

    finally:
        # Close connections
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
