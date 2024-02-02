from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from dags.config.erp_processed_salesline_data_config import default_args
import os
import pymssql
import pandas as pd


# Load environment variables from .env file
load_dotenv()
BYOD_SERVER = os.getenv('BYOD_SERVER')
BYOD_DATABASE = os.getenv('BYOD_DATABASE')
BYOD_USERNAME = os.getenv('BYOD_USERNAME')
BYOD_PASSWORD = os.getenv('BYOD_PASSWORD')

# Dag definition
dag = DAG(
    'erp_processed_salesline_data',
    default_args=default_args,
    description='DAG to extract processed salesline data from ERP and '
    'consolidate it in a single table in Snowflake',
    schedule_interval=timedelta(days=1),
)


# Tasks functions
def get_salesline_for_period(
        cursor, table_name, columns, date, period,
        batch_size=None, offset=None
        ):
    '''
    Retrieves sales data for a specific period from an external data source.

    Parameters:
    - cursor: Cursor for SQL queries execution.
    - table_name (str): Table name for data extraction.
    - columns (str): Columns to select in SQL query.
    - date (datetime): Reference date for query period.
    - period (str): Time period for data extraction ('day', 'month', 'year').
    - batch_size (int, optional): Records per batch; all data if None.
    - offset (int, optional): Dataset start offset for query.

    Retrieves sales data for specified periods, useful for large dataset
    chunks.
    '''
    duplicates_select = f'''
        SELECT SALESID FROM {table_name}
        GROUP BY LINENUM, SALESID, EXTERNALITEMID, ITEMID, CONFIGID,
            INVENTCOLORID, INVENTSIZEID, INVENTSTYLEID, INVENTSTATUSID,
            INVENTLOCATIONID, QTY
        HAVING COUNT(*) > 1'''

    if period == 'day':
        date_filter =  \
            f"CAST(SYNCSTARTDATETIME AS DATE) = '{date.strftime('%Y-%m-%d')}'"
    elif period == 'month':
        date_filter = \
            (f'YEAR(SYNCSTARTDATETIME) = {date.year} AND'
             f' MONTH(SYNCSTARTDATETIME) = {date.month}')
    elif period == 'year':
        date_filter = f'YEAR(SYNCSTARTDATETIME) = {date.year}'

    query = f'''
    SELECT {columns} FROM {table_name}
    WHERE {date_filter}
    AND SALESID NOT IN (
        {duplicates_select}
    )
    '''
    if (batch_size and offset) is not None:
        query += f''' ORDER BY SALESID
                OFFSET {offset} ROWS
                FETCH NEXT {batch_size} ROWS ONLY'''
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
    df['CREATEDTRANSACTIONDATE2'] = \
        df['CREATEDTRANSACTIONDATE2'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['INVOICEDATE'] = df['INVOICEDATE'].dt.strftime('%Y-%m-%d')
    df['LINENUM'] = df['LINENUM'].fillna(0).astype(int)
    df['QTY'] = df['QTY'].fillna(0).astype(int)
    df['CANALCODE'] = \
        pd.to_numeric(
            df['CANALCODE'], errors='coerce').fillna(0).astype(int)
    df['CECOCODE'] = \
        pd.to_numeric(
            df['CECOCODE'], errors='coerce').fillna(0).astype(int)
    df['TENDERTYPEID'] = \
        pd.to_numeric(
            df['TENDERTYPEID'], errors='coerce').fillna(0).astype(int)

    # Define primery key to load in snowflake
    df['SALESLINEPK'] = df.apply(
        lambda row: '-'.join([
            str(row['LINENUM']),
            str(row['INVENTTRANSID']),
            str(row['INVENTDIMID']),
            str(row['REFCUSTINVOICETRANSRECID']),
            str(row['INVOICEID']),
            str(row['SALESID']),
            str(row['EXTERNALITEMID']),
            str(row['DEV_SALESID']),
            str(row['ITEMID']),
            str(row['CONFIGID']),
            str(row['INVENTCOLORID']),
            str(row['INVENTSIZEID']),
            str(row['INVENTSTYLEID']),
            str(row['INVENTSTATUSID']),
            str(row['INVENTLOCATIONID']),
            str(row['QTY'])
        ]), axis=1
    )
    print('[AIRFLOW] Dataframe processed')
    print(df.head())
    print(df.shape)
    return df


def get_massive_byod_processed_salesline(
        start_date, end_date, period='day', batch_size=100000
        ):
    '''
    Processes and loads large sales data volumes from an external source to
    Snowflake in defined batches.

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
    print(f'[Start execution] Get erp salesline '
          f'from {start_date} to {end_date} by {period}')
    conn = pymssql.connect(
        BYOD_SERVER, BYOD_USERNAME, BYOD_PASSWORD, BYOD_DATABASE
    )
    table_name = default_args['byod_erp_sales_table_name']
    columns = ', '.join([
        column[0]
        for column in default_args[
            'snowflake_erp_processed_salesline_table_columns']
        if column[0] != 'SALESLINEPK'
    ])

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
            df = get_salesline_for_period(
                cursor, table_name, columns, current_date,
                period, batch_size, offset
            )
            if df is not None and not df.empty:
                processed_df = process_data(df)
                print(f'''[Airflow] Batch processing
                        - Batch size: {batch_size}
                        - Offset {offset}''') if batch_size else 0

                print('[SNOWFLAKE] Write data')
                write_data_to_snowflake(
                    processed_df, 'ERP_PROCESSED_SALESLINE',
                    default_args[
                        'snowflake_erp_processed_salesline_table_columns'
                    ],
                    'SALESLINEPK',
                    'TEMP_ERP_PROCESSED_SALESLINE'
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


def write_data_to_snowflake(
        df, table_name, columns, primary_key, temporary_table_name
        ):
    '''
    Writes a Pandas DataFrame to a Snowflake table.

    Parameters:
    - df (pandas.DataFrame): DataFrame to be written to Snowflake.
    - table_name (str): Name of the target table in Snowflake.

    Utilizes the SnowflakeHook from Airflow to establish a connection.
    The write_pandas method from snowflake-connector-python is used to
    write the DataFrame.
    '''
    # Use the SnowflakeHook to get a connection object
    hook = SnowflakeHook(snowflake_conn_id='patagonia_snowflake_connection')
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Create Temporary Table
        create_snowflake_temporary_table(
            cursor,
            temporary_table_name,
            columns
        )

        # Write the DataFrame to Snowflake Temporary Table
        success, nchunks, nrows, _ = \
            write_pandas(conn, df, temporary_table_name)
        if not success:
            raise Exception(f'Failed to write to {table_name} in Snowflake.')
        print(f'Successfully wrote {nrows} rows to '
              f'{temporary_table_name} in Snowflake.')

        # Generate UPDATE y INSERT by snowflake_shopify_customer_table_columns
        update_set_parts = []
        insert_columns = []
        insert_values = []

        for column, _ in columns:
            update_set_parts.append(
                f'{table_name}.{column} = new_data.{column}')
            insert_columns.append(column)
            insert_values.append(f'new_data.{column}')

        update_set_sql = ',\n'.join(update_set_parts)
        insert_columns_sql = ', '.join(insert_columns)
        insert_values_sql = ', '.join(insert_values)

        # Snowflake Merge execute
        cursor.execute('BEGIN')
        merge_sql = f'''
        MERGE INTO {table_name} USING {temporary_table_name} AS new_data
        ON {table_name}.{primary_key} = new_data.{primary_key}
        WHEN MATCHED THEN
            UPDATE SET
                {update_set_sql},
                snowflake_updated_at = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns_sql},
                    snowflake_created_at, snowflake_updated_at)
            VALUES ({insert_values_sql},
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
         '''
        cursor.execute(merge_sql)

        duplicates = check_duplicates_sql(cursor, table_name, primary_key)
        if duplicates:
            cursor.execute('ROLLBACK')
            print(f'There are duplicates: {duplicates}. ROLLBACK executed.')
        else:
            cursor.execute('COMMIT')
            cursor.execute(f'''SELECT COUNT(*) FROM {table_name}
                           WHERE DATE(snowflake_created_at) = CURRENT_DATE''')
            new_rows = cursor.fetchone()
            cursor.execute(f'''SELECT COUNT(*) FROM {table_name}
                           WHERE DATE(snowflake_updated_at) = CURRENT_DATE
                            AND DATE(snowflake_created_at) <> CURRENT_DATE''')
            updated_rows = cursor.fetchone()

            print(f'''
            [EXECUTION RESUME]
            Table {table_name} modified successfully!
            - New inserted rows: {new_rows[0]}
            - Updated rows: {updated_rows[0]}
            ''')

    except Exception as e:
        cursor.execute('ROLLBACK')
        raise e
    finally:
        cursor.close()


def check_duplicates_sql(cursor, table_name, primary_key):
    '''
    Checks for duplicate records in a specified Snowflake table.

    This function executes an SQL query to identify duplicate entries
    based on the SHOPIFY_ID column.

    Parameters:
    - cursor: A database cursor to execute the query in Snowflake.
    - table_name (str): The name of the table to check for duplicates.

    Returns:
    - list: A list of tuples containing the SHOPIFY_IDs and the count
    of their occurrences, if duplicates are found.

    The function executes an SQL query that groups records by SHOPIFY_ID
    and counts occurrences, looking for counts greater than one.
    If duplicates are found, it returns the list of these records.
    In case of an exception, it performs a rollback and prints the error.
    '''
    check_duplicates_sql = f'''
    SELECT {primary_key}, COUNT(*)
    FROM {table_name}
    GROUP BY {primary_key}
    HAVING COUNT(*) > 1;
    '''
    try:
        cursor.execute(check_duplicates_sql)
        return cursor.fetchall()
    except Exception as e:
        cursor.execute('ROLLBACK')
        print('ROLLBACK executed due to an error:', e)


def run_get_byod_processed_salesline(**context):
    '''
    Executes the Airflow task, setting start and end dates for data extraction
    and calling `get_massive_byod_processed_salesline`.

    Start date is three days before the current date, end date is current date.

    Parameters:
    - context (dict): Execution context with metadata and settings for DAG run.

    No return value. Executes data extraction and loads results into Snowflake.
    '''
    execution_date = context['execution_date']
    print(f'Execution Date: {execution_date}')

    end_date = datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    start_date = end_date - timedelta(days=3)

    get_massive_byod_processed_salesline(
        start_date, end_date, period='day', batch_size=50000
    )


# Task definitions
task_1 = PythonOperator(
    task_id='get_byod_processed_salesline',
    python_callable=run_get_byod_processed_salesline,
    dag=dag,
)
