from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from config.erp_inventory_data_config import default_args
from utils.utils import write_data_to_snowflake, truncate_table
import os
import pymssql
import pandas as pd
import requests

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


load_dotenv()
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')
BYOD_SERVER = os.getenv('BYOD_SERVER')
BYOD_DATABASE = os.getenv('BYOD_DATABASE')
BYOD_USERNAME = os.getenv('BYOD_USERNAME')
BYOD_PASSWORD = os.getenv('BYOD_PASSWORD')
WMS_API_URL = os.getenv('WMS_API_URL')
WMS_API_USER = os.getenv('WMS_API_USER')
WMS_API_PASSWORD = os.getenv('WMS_API_PASSWORD')


# DAG definition
dag = DAG(
    'erp_and_wms_inventory_data',
    default_args=default_args,
    description='DAG to extract inventory data from'
    ' ERP and WMS, and load it into Snowflake',
    schedule_interval='0 9 * * *',
    catchup=False,
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


def get_wms_inventory():
    print('[Start execution] Fetching inventory data from WMS')

    url = f'{WMS_API_URL}/Users/authenticate'
    my_obj = {'username': WMS_API_USER, 'password': WMS_API_PASSWORD}
    r = requests.post(url, json=my_obj, verify=False)

    if r.status_code != 200:
        raise Exception('Failed to authenticate with WMS API')

    token = r.json()['jwtToken']
    headers = {'Authorization': token}
    url = f'{WMS_API_URL}/api/GetStockOwner?owner=PAT'
    r2 = requests.get(url, headers=headers, verify=False)

    if r2.status_code != 200:
        raise Exception('Failed to fetch data from WMS')

    df_wms = pd.DataFrame(r2.json())

    # Delete unnecessary columns
    columns_to_drop = [
        'idWhs', 'whsCode', 'shortWhsName', 'ownCode', 'ownName'
    ]
    df_wms.drop(columns=columns_to_drop, inplace=True)
    df_wms.columns = [col.upper() for col in df_wms.columns]
    df_wms.drop_duplicates(inplace=True)

    print(df_wms.head())
    print(df_wms.shape)
    return df_wms


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

        truncate_table('ERP_INVENTORY', SNOWFLAKE_CONN_ID)
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


def run_get_wms_inventory_data(**context):
    df_wms = get_wms_inventory()
    if not df_wms.empty:
        write_data_to_snowflake(
            df_wms,
            'WMS_INVENTORY_HISTORY',
            default_args['wms_inventory_table_columns'],
            ['IDITEM', 'ITEMCODE', 'FECHAREGISTRO'],
            'TEMP_WMS_INVENTORY_HISTORY',
            SNOWFLAKE_CONN_ID
        )

        truncate_table('WMS_INVENTORY', SNOWFLAKE_CONN_ID)
        write_data_to_snowflake(
            df_wms,
            'WMS_INVENTORY',
            default_args['wms_inventory_table_columns'],
            ['IDITEM', 'ITEMCODE'],
            'TEMP_WMS_INVENTORY',
            SNOWFLAKE_CONN_ID
        )


# Tasks
task_1 = PythonOperator(
    task_id='get_inventory_data',
    python_callable=run_get_inventory_data,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='get_wms_inventory_data',
    python_callable=run_get_wms_inventory_data,
    dag=dag,
)

# Task dependencies
task_1 >> task_2
