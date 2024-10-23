from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from dags.config.erp_inventory_data_config import default_args
from dags.utils.utils import write_data_to_snowflake
import os
import pymssql
import pandas as pd


load_dotenv()
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')
BYOD_SERVER = os.getenv('BYOD_SERVER')
BYOD_DATABASE = os.getenv('BYOD_DATABASE')
BYOD_USERNAME = os.getenv('BYOD_USERNAME')
BYOD_PASSWORD = os.getenv('BYOD_PASSWORD')

# DAG definition
dag = DAG(
    'erp_inventory_data',
    default_args=default_args,
    description='DAG to extract inventory data from'
    ' ERP and load it into Snowflake',
    schedule_interval='0 9 * * *',
    catchup=False
)


def get_byod_inventory():
    '''
    Fetches inventory data from ERP based on the defined columns of interest.

    Establishes a connection to the database using the provided credentials
    and executes an SQL query that retrieves only the relevant columns
    specified in the DAG configuration.

    Returns:
        pd.DataFrame: A DataFrame containing the inventory data.
                      Returns an empty DataFrame if no results are found.
    '''
    print('[Start execution] Fetching inventory data from ERP')

    conn = pymssql.connect(
        BYOD_SERVER, BYOD_USERNAME, BYOD_PASSWORD, BYOD_DATABASE
    )
    cursor = conn.cursor(as_dict=True)

    # Build SQL query using the columns of interest
    table_name = default_args['byod_erp_inventory_table_name']
    columns = ', '.join([
        column[0]
        for column in default_args['byod_erp_inventory_table_interest_columns']
    ])
    query = f'SELECT {columns} FROM {table_name}'

    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()

    if not result:
        return pd.DataFrame()

    df_inventory = pd.DataFrame(result)

    if 'SYNCSTARTDATETIME' in df_inventory.columns:
        df_inventory['SYNCSTARTDATETIME'] = \
            pd.to_datetime(
                df_inventory['SYNCSTARTDATETIME']
                ).dt.strftime('%Y-%m-%d %H:%M:%S')

    # Create SKU column with ITEMNUMBER, PRODUCTCOLORID, and PRODUCTSIZEID
    df_inventory['SKU'] = df_inventory.apply(
        lambda row:
        f"{row['ITEMNUMBER']}-{row['PRODUCTCOLORID']}-{row['PRODUCTSIZEID']}",
        axis=1
    )

    print(df_inventory.head())
    print(df_inventory.shape)
    return df_inventory


def run_get_inventory_data(**context):
    '''
    Executes the process to fetch inventory data from ERP and load
    it into Snowflake.

    If the retrieved DataFrame is not empty, it will be written into the
    `ERP_INVENTORY` table in Snowflake using the defined columns and
    key fields.
    '''
    df_inventory = get_byod_inventory()
    if not df_inventory.empty:
        write_data_to_snowflake(
            df_inventory,
            'ERP_INVENTORY_HISTORY',
            default_args['byod_erp_inventory_table_columns'],
            ['SKU', 'PRODUCTSTYLEID', 'INVENTORYWAREHOUSEID',
             'INVENTORYSTATUSID', 'SYNCSTARTDATETIME'],
            'TEMP_ERP_INVENTORY_HISTORY',
            SNOWFLAKE_CONN_ID
        )

        write_data_to_snowflake(
            df_inventory,
            'ERP_INVENTORY',
            default_args['byod_erp_inventory_table_columns'],
            ['SKU', 'PRODUCTSTYLEID',
             'INVENTORYWAREHOUSEID',
             'INVENTORYSTATUSID'],
            'TEMP_ERP_INVENTORY',
            SNOWFLAKE_CONN_ID
        )


task_1 = PythonOperator(
    task_id='get_inventory_data',
    python_callable=run_get_inventory_data,
    dag=dag,
)
