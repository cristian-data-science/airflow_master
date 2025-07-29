from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from dotenv import load_dotenv
from config.erp_accounting_transactions_data_config import default_args
from utils.utils import write_data_to_snowflake
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

# Configurable variables from Airflow UI with default values
DAYS = int(Variable.get("erp_accounting_days", default_var="10"))
PERIOD = Variable.get("erp_accounting_period", default_var="day")
BATCH_SIZE = int(Variable.get(
    "erp_accounting_batch_size", default_var="10000"))


# Dag definition
dag = DAG(
    'erp_accounting_transactions_data',
    default_args=default_args,
    description='DAG to extract accounting transactions data from ERP and '
    'consolidate it in a single table in Snowflake',
    schedule_interval='45 10 * * *',
    catchup=False,
    tags=['erp', 'accounting']
)


# Tasks functions
def get_transactions_for_period(
        cursor, table_name, date, period,
        batch_size=None, offset=None
        ):
    '''
    Retrieves transactions data for a specific period
    from an external data source.

    Parameters:
    - cursor: Cursor for SQL queries execution.
    - table_name (str): Table name for data extraction.
    - date (datetime): Reference date for query period.
    - period (str): Time period for data extraction ('day', 'month', 'year').
    - batch_size (int, optional): Records per batch; all data if None.
    - offset (int, optional): Dataset start offset for query.

    Retrieves transactions data for specified periods, useful for large dataset
    chunks.
    '''
    if period == 'day':
        date_filter =  \
            f"CAST(ACCOUNTINGDATE AS DATE) = '{date.strftime('%Y-%m-%d')}'"
    elif period == 'month':
        date_filter = \
            (f'YEAR(ACCOUNTINGDATE) = {date.year} AND'
             f' MONTH(ACCOUNTINGDATE) = {date.month}')
    elif period == 'year':
        date_filter = f'YEAR(ACCOUNTINGDATE) = {date.year}'

    query = f'''
    SELECT * FROM {table_name}
    WHERE {date_filter}
    '''
    if (batch_size and offset) is not None:
        query += f''' ORDER BY GENERALJOURNALACCOUNTENTRY_RECID
                OFFSET {offset} ROWS
                FETCH NEXT {batch_size} ROWS ONLY'''
    print(f'BATCH SIZE: {batch_size}')
    print('[BYOD] Executing query')
    cursor.execute(query)
    print('[BYOD] Query finished')
    result = cursor.fetchall()
    return pd.DataFrame(result) if result else None


def process_data(df):
    '''
    Processes the DataFrame by formatting date columns and generating a primary
    key.

    Parameters:
    - df (pandas.DataFrame): DataFrame containing the sales data.

    The function formats date columns into a specific string format and creates
    a primary key column 'SALESLINEPK'. It fills missing values in certain
    columns and converts them to the appropriate data type.

    Returns:
    - pandas.DataFrame: The processed DataFrame ready for further operations.
    '''
    print('[AIRFLOW] Dataframe processing started')
    df['SYNCSTARTDATETIME'] = \
        df['SYNCSTARTDATETIME'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['ACCOUNTINGDATE'] = df['ACCOUNTINGDATE'].dt.strftime('%Y-%m-%d')
    df['DOCUMENTDATE'] = df['DOCUMENTDATE'].dt.strftime('%Y-%m-%d')
    df['ISSELECTED'] = df['ISSELECTED'].fillna(0).astype(int).astype(bool)
    df['TRANSFERSTATUS'] = \
        df['TRANSFERSTATUS'].fillna(0).astype(int).astype(bool)
    print('[AIRFLOW] Dataframe processed')
    print(df.head())
    print(df.shape)
    return df


def get_massive_byod_accounting_transactions(
        start_date, end_date, period='day', batch_size=100000
        ):
    '''
    Processes and loads large transactions data volumes from an
    external source to Snowflake in defined batches.

    Parameters:
    - start_date (datetime): Start date for data processing range.
    - end_date (datetime): End date for data processing range.
    - period (str): Time period for data extraction ('day', 'month', 'year').
    - batch_size (int, optional): Records per batch.
        If None, processes all data.

    Iterates over date range, extracts, processes, and loads data into
    Snowflake. If batch_size is specified, processes data in those
    batch sizes.
    '''
    print(f'[Start execution] Get erp accounting transactions'
          f'from {start_date} to {end_date} by {period}')
    conn = pymssql.connect(
        BYOD_SERVER, BYOD_USERNAME, BYOD_PASSWORD, BYOD_DATABASE
    )
    table_name = default_args['byod_erp_accounting_table_name']
    current_date = start_date
    while current_date <= end_date:
        # We close the conn each write_data_to_snowflake call
        cursor = conn.cursor(as_dict=True)

        print(f'''
        [NEW EXECUTION]
        - DAY TO PROCESS: {current_date}'
        - PERIOD: {period}''')
        offset = 0
        while True:
            df = get_transactions_for_period(
                cursor, table_name, current_date,
                period, batch_size, offset
            )
            if df is not None and not df.empty:
                processed_df = process_data(df)
                print(f'''[Airflow] Batch processing
                        - Batch size: {batch_size}
                        - Offset {offset}''') if batch_size else 0

                print('[SNOWFLAKE] Write data')
                write_data_to_snowflake(
                    processed_df,
                    'ERP_ACCOUNTING_TRANSACTION',
                    default_args[
                        'snowflake_erp_accounting_table_columns'],
                    ['GENERALJOURNALACCOUNTENTRY_RECID'],
                    'TEMP_ERP_ACCOUNTING_TRANSACTION',
                    SNOWFLAKE_CONN_ID,
                )
                if batch_size is not None:
                    offset += batch_size
                else:
                    break
            else:
                print('[BYOD] Query result empty!')
                break

        # Update current_date based on the period
        if period == 'day':
            current_date += timedelta(days=1)
        elif period == 'month':
            next_month = \
                current_date.month + 1 if current_date.month < 12 else 1
            next_year = \
                current_date.year + 1 if next_month == 1 else current_date.year
            current_date = \
                current_date.replace(year=next_year, month=next_month, day=1)
        elif period == 'year':
            current_date = \
                current_date.replace(
                    year=current_date.year + 1, month=1, day=1
                )


def run_get_byod_accounting_transactions(**context):
    '''
    Executes the Airflow task, setting start and end dates for data extraction
    and calling `get_byod_accounting_transactions`.

    Start date is three days before the current date, end date is current date.

    Parameters:
    - context (dict): Execution context with metadata and settings for DAG run.

    No return value. Executes data extraction and loads results into Snowflake.
    '''
    execution_date = context['execution_date']
    print(f'Execution Date: {execution_date}')
    
    # Show configuration values being used
    print('[Airflow] Configuration values:')
    print(f'  - DAYS: {DAYS}')
    print(f'  - PERIOD: {PERIOD}')
    print(f'  - BATCH_SIZE: {BATCH_SIZE}')

    end_date = datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    start_date = end_date - timedelta(days=DAYS)
    
    print(f'[Airflow] Processing accounting transactions from '
          f'{start_date} to {end_date}')

    get_massive_byod_accounting_transactions(
        start_date, end_date, period=PERIOD, batch_size=BATCH_SIZE
    )


# Task definitions
task_1 = PythonOperator(
    task_id='get_byod_accounting_transactions',
    python_callable=run_get_byod_accounting_transactions,
    dag=dag,
)

task_1
