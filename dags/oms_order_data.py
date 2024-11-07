from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from dotenv import load_dotenv
from config.oms_order_data_config import default_args
from utils.utils import write_data_to_snowflake
import os
import requests
from requests.exceptions import HTTPError, ChunkedEncodingError
import time
import pandas as pd

# Load environment variables from .env file
load_dotenv()
OMS_API_CLIENT_ID = os.getenv('OMS_API_CLIENT_ID')
OMS_API_CLIENT_SECRET = os.getenv('OMS_API_CLIENT_SECRET')
OMS_API_URL = os.getenv('OMS_API_URL')
OMS_API_INSTANCE = os.getenv('OMS_API_INSTANCE')
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')
OMS_TOKEN_URL = f'{OMS_API_URL}authentication/oauth2/token'
OMS_ORDER_URL = f'{OMS_API_URL}{OMS_API_INSTANCE}/powerbi/order'

DAYS = 2
BATCH_LIMIT = 20
TOTAL_LIMIT = 2000
DB_WRITE_BATCH_SIZE = 200
MAX_RETRIES = 5

# Dag definition
dag = DAG(
    'oms_order_data',
    default_args=default_args,
    description='DAG to extract order data from OMS '
    'and write in Snowflake',
    schedule_interval='0 */4 * * *',
    catchup=False,
)


class OMSDataFetcher:

    def __init__(self):
        print('Ejecutó init')
        self.auth_token = None
        self.get_auth_token()

    def get_auth_token(self):
        data = {
            'grant_type': 'client_credentials',
            'client_id': OMS_API_CLIENT_ID,
            'client_secret': OMS_API_CLIENT_SECRET
        }
        response = requests.post(OMS_TOKEN_URL, data=data)
        response.raise_for_status()
        self.auth_token = response.json()['access_token']

    def fetch_oms_orders(self, batch_limit=20, total_limit=50,
                         db_write_batch_size=100, max_retries=5):
        headers = {'Authorization': f'Bearer {self.auth_token}'}
        orders = []
        offset = 0
        requests_count = 0
        total_fetched = 0
        retries = 0

        while True:
            if not self.auth_token:
                self.get_auth_token()
                headers['Authorization'] = f'Bearer {self.auth_token}'

            try:
                date_limit = (
                    datetime.now() - timedelta(days=DAYS)
                    ).replace(
                        hour=0, minute=0, second=0, microsecond=0
                    ).isoformat()
                filters = str([
                    ('state_write_date', '>', date_limit)
                ])
                params = {
                    'limit': batch_limit,
                    'offset': offset,
                    'filters': filters
                }
                print(f'[OMS] Getting {batch_limit} orders - offset: {offset}')
                print(f'[OMS] Request count: {requests_count}')
                response = requests.get(
                    OMS_ORDER_URL, headers=headers, params=params
                )
                requests_count += 1

                response.raise_for_status()
                batch = response.json().get('data', [])
                orders.extend(batch)
                total_fetched += len(batch)

                if len(orders) >= db_write_batch_size:
                    print(f'To process {len(orders)} orders')
                    self.process_orders(orders)
                    orders = []

                if not batch or (total_limit and total_fetched >= total_limit):
                    break

                offset += batch_limit
                time.sleep(1)

            except HTTPError as e:
                if e.response.status_code == 401:  # Token expired
                    print('Token expired. Obtaining new token...')
                    self.get_auth_token()
                    headers['Authorization'] = f'Bearer {self.auth_token}'
                    continue
                retries += 1
                if max_retries > 0:
                    print(f'Retrying... attempt {retries}')
                    time.sleep(2 ** retries)  # Exponential backoff
                else:
                    print('Max retries exceeded. Failing the task.')
                    raise e
            except (ConnectionError, ChunkedEncodingError) as e:
                print(f'Connection error encountered: {str(e)}')
                retries += 1
                if retries < max_retries:
                    print(f'Retrying... attempt {retries}')
                    time.sleep(2 ** retries)  # Exponential backoff
                else:
                    print('Max retries exceeded. Failing the task.')
                    raise e
            except Exception as e:
                print(f'Unhandled exception: {str(e)}')
                raise e

        if orders:
            print(f'Processing the last batch of {len(orders)} orders')
            self.process_orders(orders)
        return orders

    def process_orders(self, orders_list):
        orders_dataframe = \
            self.orders_to_dataframe(orders_list)

        print('[AIRFLOW] Orders Dataframe: ')
        print(orders_dataframe.head().to_string())

        write_data_to_snowflake(
            orders_dataframe,
            'OMS_SUBORDERSLINE',
            default_args['snowflake_oms_suborder_line_data_table_columns'],
            ['LINE_ID'],
            'TEMP_OMS_SUBORDERSLINE',
            SNOWFLAKE_CONN_ID
        )

        orders_status_dataframe = \
            self.extract_status_histories_to_dataframe(orders_list)

        print('[AIRFLOW] Orders Status Dataframe: ')
        print(orders_status_dataframe.head(20).to_string())

        write_data_to_snowflake(
            orders_status_dataframe,
            'OMS_SUBORDER_STATUS_HISTORY',
            default_args[
                'snowflake_oms_suborder_status_history_data_table_columns'],
            ['PRIMARY_KEY'],
            'TEMP_OMS_SUBORDER_STATUS_HISTORY',
            SNOWFLAKE_CONN_ID
        )

    def orders_to_dataframe(self, orders):
        orders_data = []
        for order in orders:
            transfer_codes = {}
            for trans in order.get('transfer_warehouse_ids', []):
                sku = trans['product_id']['sku']
                warehouse_name = trans['warehouse_name']
                if sku in transfer_codes:
                    transfer_codes[sku].add(warehouse_name)
                else:
                    transfer_codes[sku] = {warehouse_name}
            for line in order.get('order_line', []):
                transfer_warehouse = ', '.join(
                    transfer_codes.get(line['product_id']['default_code'], [])
                )
                orders_data.append({
                    'LINE_ID': line['id'],
                    'SUBORDER_ID': order['id'],
                    'ORDER_ID': order['ecommerce_sale_id']['id'],
                    'AMOUNT_DISCOUNT': order['amount_discount'],
                    'AMOUNT_TAX': order['amount_tax'],
                    'AMOUNT_TOTAL': order['amount_total'],
                    'AMOUNT_UNTAXED': order['amount_untaxed'],
                    'STATE_OPTION_NAME':
                        order['current_state_id']['state_option_id']['name'],
                    'DATE_ORDER': order['date_order'],
                    'DELIVERY_CLIENT_DATE': order['delivery_client_date'],
                    'DELIVERY_METHOD_NAME':
                        order['delivery_method_id']['name'],
                    'ECOMMERCE_DATE_ORDER': order['ecommerce_date_order'],
                    'ECOMMERCE_NAME': order['ecommerce_name'],
                    'ECOMMERCE_NAME_CHILD': order['ecommerce_name_child'],
                    'CITY_NAME':
                        order['ecommerce_sale_id'].get(
                            'shipment_address_id', {}
                            ).get('city_id', {}).get('name', ''),
                    'STREET':
                        order['ecommerce_sale_id'].get(
                            'shipment_address_id', {}).get('street', ''),
                    'PHONE': order['ecommerce_sale_id'].get(
                        'shipment_address_id', {}).get('phone', ''),
                    'STATE_NAME': order['ecommerce_sale_id'].get(
                        'shipment_address_id', {}).get(
                            'state_id', {}).get('name', ''),
                    'DISCOUNT': line['discount'],
                    'PRICE_REDUCE': line['price_reduce'],
                    'PRICE_TOTAL': line['price_total'],
                    'PRICE_UNIT': line['price_unit'],
                    'PRODUCT_UOM_QTY': line['product_uom_qty'],
                    'DEFAULT_CODE': line['product_id']['default_code'],
                    'PRODUCT_NAME': line['product_id']['name'],
                    'EMAIL':
                        order.get('partner_id', {}).get('email', ''),
                    'PARTNER_NAME':
                        order.get('partner_id', {}).get('name', ''),
                    'PARTNER_PHONE':
                        order.get('partner_id', {}).get('phone', ''),
                    'PARTNER_STREET':
                        order.get('partner_id', {}).get('street', ''),
                    'PARTNER_VAT':
                        order.get('partner_id', {}).get('vat', ''),
                    'PAYMENT_METHOD_NAME':
                        order.get('payment_method_id', {}).get('name', ''),
                    'WAREHOUSE': order['warehouse_id']['name'],
                    'TRANSFER_WAREHOUSE': transfer_warehouse
                })
        df = pd.DataFrame(orders_data)
        df['PARTNER_VAT'] = df['PARTNER_VAT'].astype(
            str).replace('nan', '').replace('False', '')
        df = df.drop_duplicates(subset=['LINE_ID'], keep='first')
        return df

    def extract_status_histories_to_dataframe(self, orders):
        status_histories = []
        for order in orders:
            for status in order.get('status_histories_ids', []):
                if status['status']:
                    suborder_id = order['ecommerce_name_child']
                    status_name = status['status']
                    register_date = status['create_date']
                    status_histories.append({
                        'PRIMARY_KEY': (
                            f'{suborder_id}-{status_name}-{register_date}'
                        ),
                        'ECOMMERCE_NAME_CHILD': suborder_id,
                        'STATUS': status_name,
                        'REGISTER_DATE': register_date
                    })
        df = pd.DataFrame(status_histories)
        return df.drop_duplicates(subset=['PRIMARY_KEY'], keep='first')


def run_get_oms_orders(**context):
    execution_date = context['execution_date']
    print(f'Execution Date: {execution_date}')
    fetcher = OMSDataFetcher()
    fetcher.fetch_oms_orders(
        batch_limit=BATCH_LIMIT, total_limit=TOTAL_LIMIT,
        db_write_batch_size=DB_WRITE_BATCH_SIZE, max_retries=MAX_RETRIES
    )


def process_oms_suborders(start_date):
    '''
    Updates the OMS_SUBORDERS table with aggregated order data from
    OMS_SUBORDERSLINE table within the specified date range.

    Parameters:
    - start_date (datetime): Start date for the data processing range.
    - end_date (datetime): End date for the data processing range.
    '''

    sql_query = f"""
    ALTER SESSION SET TIMEZONE = 'America/Santiago';
    MERGE INTO OMS_SUBORDERS target
    USING (
        SELECT
            SUBORDER_ID,
            MAX(ORDER_ID) AS ORDER_ID,
            MAX(DATE_ORDER) AS ORDER_DATE,
            SUM(AMOUNT_DISCOUNT) AS TOTAL_DISCOUNT,
            SUM(AMOUNT_TAX) AS TOTAL_TAX,
            SUM(AMOUNT_TOTAL) AS TOTAL_AMOUNT,
            SUM(AMOUNT_UNTAXED) AS TOTAL_UNTAXED,
            MAX(STATE_OPTION_NAME) AS STATE,
            MAX(DELIVERY_CLIENT_DATE) AS DELIVERY_DATE,
            MAX(DELIVERY_METHOD_NAME) AS DELIVERY_METHOD,
            MAX(ECOMMERCE_NAME) AS ECOMMERCE_NAME,
            MAX(ECOMMERCE_NAME_CHILD) AS ECOMMERCE_NAME_CHILD,
            MAX(WAREHOUSE) AS WAREHOUSE,
            COUNT(*) AS NUMBER_OF_ITEMS,
            SUM(PRODUCT_UOM_QTY) AS NUMBER_OF_PRODUCTS,
            MAX(CITY_NAME) AS CITY_NAME,
            MAX(STREET) AS STREET,
            MAX(PHONE) AS PHONE,
            MAX(STATE_NAME) AS STATE_NAME,
            MAX(DISCOUNT) AS DISCOUNT,
            SUM(PRICE_REDUCE) AS PRICE_REDUCE,
            SUM(PRICE_TOTAL) AS PRICE_TOTAL,
            MAX(EMAIL) AS EMAIL,
            MAX(PARTNER_NAME) AS PARTNER_NAME,
            MAX(PARTNER_VAT) AS PARTNER_VAT,
            MAX(PAYMENT_METHOD_NAME) AS PAYMENT_METHOD_NAME,
            LISTAGG(
                NULLIF(TRANSFER_WAREHOUSE, ''), ', ')
                WITHIN GROUP (ORDER BY SUBORDER_ID)
                AS TRANSFER_WAREHOUSE
        FROM OMS_SUBORDERSLINE
        WHERE SNOWFLAKE_UPDATED_AT
            > '{start_date.strftime("%Y-%m-%d")}'
        GROUP BY SUBORDER_ID
    ) AS source
    ON target.SUBORDER_ID = source.SUBORDER_ID
    WHEN MATCHED THEN
        UPDATE SET
            ORDER_ID = source.ORDER_ID,
            ORDER_DATE = source.ORDER_DATE,
            TOTAL_DISCOUNT = source.TOTAL_DISCOUNT,
            TOTAL_TAX = source.TOTAL_TAX,
            TOTAL_AMOUNT = source.TOTAL_AMOUNT,
            TOTAL_UNTAXED = source.TOTAL_UNTAXED,
            STATE = source.STATE,
            DELIVERY_DATE = source.DELIVERY_DATE,
            DELIVERY_METHOD = source.DELIVERY_METHOD,
            ECOMMERCE_NAME = source.ECOMMERCE_NAME,
            ECOMMERCE_NAME_CHILD = source.ECOMMERCE_NAME_CHILD,
            WAREHOUSE = source.WAREHOUSE,
            NUMBER_OF_ITEMS = source.NUMBER_OF_ITEMS,
            NUMBER_OF_PRODUCTS = source.NUMBER_OF_PRODUCTS,
            CITY_NAME = source.CITY_NAME,
            STREET = source.STREET,
            PHONE = source.PHONE,
            STATE_NAME = source.STATE_NAME,
            DISCOUNT = source.DISCOUNT,
            PRICE_REDUCE = source.PRICE_REDUCE,
            PRICE_TOTAL = source.PRICE_TOTAL,
            EMAIL = source.EMAIL,
            PARTNER_NAME = source.PARTNER_NAME,
            PARTNER_VAT = source.PARTNER_VAT,
            PAYMENT_METHOD_NAME = source.PAYMENT_METHOD_NAME,
            TRANSFER_WAREHOUSE = source.TRANSFER_WAREHOUSE,
            SNOWFLAKE_UPDATED_AT = CURRENT_TIMESTAMP
    WHEN NOT MATCHED THEN
        INSERT (
            SUBORDER_ID, ORDER_ID, ORDER_DATE, TOTAL_DISCOUNT, TOTAL_TAX,
            TOTAL_AMOUNT, TOTAL_UNTAXED, STATE, DELIVERY_DATE, DELIVERY_METHOD,
            ECOMMERCE_NAME, ECOMMERCE_NAME_CHILD, WAREHOUSE, NUMBER_OF_ITEMS,
            NUMBER_OF_PRODUCTS, CITY_NAME, STREET, PHONE, STATE_NAME, DISCOUNT,
            PRICE_REDUCE, PRICE_TOTAL, EMAIL, PARTNER_NAME, PARTNER_VAT,
            PAYMENT_METHOD_NAME, TRANSFER_WAREHOUSE,
            SNOWFLAKE_CREATED_AT, SNOWFLAKE_UPDATED_AT
        )
        VALUES (
            source.SUBORDER_ID, source.ORDER_ID, source.ORDER_DATE,
            source.TOTAL_DISCOUNT, source.TOTAL_TAX,
            source.TOTAL_AMOUNT, source.TOTAL_UNTAXED,
            source.STATE, source.DELIVERY_DATE, source.DELIVERY_METHOD,
            source.ECOMMERCE_NAME, source.ECOMMERCE_NAME_CHILD,
            source.WAREHOUSE, source.NUMBER_OF_ITEMS,
            source.NUMBER_OF_PRODUCTS, source.CITY_NAME, source.STREET,
            source.PHONE, source.STATE_NAME, source.DISCOUNT,
            source.PRICE_REDUCE, source.PRICE_TOTAL, source.EMAIL,
            source.PARTNER_NAME, source.PARTNER_VAT,
            source.PAYMENT_METHOD_NAME, source.TRANSFER_WAREHOUSE,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        );
    """

    return SnowflakeOperator(
        task_id='process_oms_suborders',
        sql=sql_query,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        autocommit=True,
        dag=dag
    )


# Task definitions
task_1 = PythonOperator(
    task_id='get_oms_orders',
    python_callable=run_get_oms_orders,
    dag=dag,
)

end_date = datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0
    )
start_date = end_date - timedelta(days=DAYS)
task_2 = process_oms_suborders(
    start_date
)


task_1 >> task_2
