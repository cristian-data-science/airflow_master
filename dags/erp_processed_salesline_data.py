from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from dotenv import load_dotenv
from config.erp_processed_salesline_data_config import default_args
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

DAYS = 3
PERIOD = 'day'
BATCH_SIZE = 10000


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
        batch_size=None, offset=None, exclude_duplicates=False
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
    '''
    if exclude_duplicates:
        duplicates_select = f'''
        SELECT SALESID FROM {table_name}
        GROUP BY LINENUM, INVENTTRANSID, INVENTDIMID, REFCUSTINVOICETRANSRECID,
                INVOICEID, SALESID, EXTERNALITEMID, DEV_SALESID, ITEMID,
                CONFIGID, INVENTCOLORID, INVENTSIZEID, INVENTSTYLEID,
                INVENTSTATUSID, INVENTLOCATIONID, QTY
        HAVING COUNT(*) > 1'''
        query += f'''
                AND SALESID NOT IN (
                    {duplicates_select}
                )'''
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
                    processed_df,
                    'ERP_PROCESSED_SALESLINE',
                    default_args[
                        'snowflake_erp_processed_salesline_table_columns'],
                    ['SALESLINEPK'],
                    'TEMP_ERP_PROCESSED_SALESLINE',
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
    start_date = end_date - timedelta(days=DAYS)

    get_massive_byod_processed_salesline(
        start_date, end_date, period=PERIOD, batch_size=BATCH_SIZE
    )


def process_sales_orders(start_date, end_date):
    '''
    Updates the ERP_PROCESSED_SALES table with aggregated order data from
    ERP_PROCESSED_SALESLINE table within the specified date range.

    Parameters:
    - start_date (datetime): Start date for the data processing range.
    - end_date (datetime): End date for the data processing range.
    '''

    sql_query = f"""
    MERGE INTO ERP_PROCESSED_SALES target
    USING (
        SELECT
            SALESID,
            MAX(INVOICEDATE) AS InvoiceDate,
            MAX(CREATEDTRANSACTIONDATE2) AS CreatedTransaction,
            MAX(INVOICEID) AS InvoiceID,
            MAX(CUSTACCOUNT) AS CustomerAccount,
            MAX(ORGANIZATIONNAME) AS OrganizationName,
            SUM(SALESPRICE) AS SalesPriceTotal,
            SUM(QTY) AS QtyTotal,
            MAX(CURRENCYCODE) AS CurrencyCode,
            MAX(CANAL) AS Canal,
            MAX(CECO) AS Ceco,
            MAX(TAXGROUP) AS TaxGroup,
            MAX(PURCHORDERFORMNUM) AS OrderNumber
        FROM ERP_PROCESSED_SALESLINE
        WHERE SNOWFLAKE_UPDATED_AT
            BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND
            '{end_date.strftime("%Y-%m-%d")}'
        GROUP BY SALESID
    ) AS source
    ON target.SALESID = source.SALESID
    WHEN MATCHED THEN
        UPDATE SET
            InvoiceDate = source.InvoiceDate,
            CreatedTransaction = source.CreatedTransaction,
            InvoiceID = source.InvoiceID,
            CustomerAccount = source.CustomerAccount,
            OrganizationName = source.OrganizationName,
            SalesPriceTotal = source.SalesPriceTotal,
            QtyTotal = source.QtyTotal,
            CurrencyCode = source.CurrencyCode,
            Canal = source.Canal,
            Ceco = source.Ceco,
            TaxGroup = source.TaxGroup,
            OrderNumber = source.OrderNumber
    WHEN NOT MATCHED THEN
        INSERT (
            SALESID, InvoiceDate, CreatedTransaction, InvoiceID,
            CustomerAccount, OrganizationName, SalesPriceTotal,
            QtyTotal, CurrencyCode, Canal, Ceco, TaxGroup, OrderNumber
        )
        VALUES (
            source.SALESID, source.InvoiceDate, source.CreatedTransaction,
            source.InvoiceID, source.CustomerAccount,
            source.OrganizationName, source.SalesPriceTotal, source.QtyTotal,
            source.CurrencyCode, source.Canal, source.Ceco, source.TaxGroup,
            source.OrderNumber
        );
    """

    return SnowflakeOperator(
        task_id='process_sales_orders',
        sql=sql_query,
        snowflake_conn_id='patagonia_snowflake_connection',
        autocommit=True,
        dag=dag
    )


# Task definitions
task_1 = PythonOperator(
    task_id='get_byod_processed_salesline',
    python_callable=run_get_byod_processed_salesline,
    dag=dag,
)

end_date = datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0
    )
start_date = end_date - timedelta(days=DAYS)
task_2 = process_sales_orders(
    start_date, end_date
)


task_1 >> task_2
