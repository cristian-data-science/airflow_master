from datetime import timedelta, datetime, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from dags.config.shopify_order_data_config import default_args
from dags.utils.utils import write_data_to_snowflake
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin
import os
import requests
from requests.exceptions import HTTPError, ConnectionError
import time
import pandas as pd


# Load environment variables from .env file
load_dotenv()
SHOPIFY_API_KEY = os.getenv('SHOPIFY_API_KEY')
SHOPIFY_API_PASSWORD = os.getenv('SHOPIFY_API_PASSWORD')
SHOPIFY_API_URL = \
    os.getenv('SHOPIFY_API_URL') + os.getenv('SHOPIFY_API_VERSION') + '/'
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')

BATCH_LIMIT = 10
RESPONSE_LIMIT = 10
DAYS = 1
BATCH_SIZE = 10

# Dag definition
dag = DAG(
    'shopify_orders_line',
    default_args=default_args,
    description='DAG to extract order data from Shopify '
    'and write in Snowflake',
    schedule_interval=timedelta(days=1),
)


# Tasks functions
def get_shopify_orders(
        batch_limit=250, response_limit=None,
        days=1, batch_size=100, max_retries=5
        ):
    '''
    Fetches order data from Shopify API with pagination support and filters
    based on the last updated date.

    Parameters:
    - batch_limit (int): Max number of records per API call. Defaults to 250.
    - response_limit (int, optional): Max total records to fetch. If None,
      fetches all. Defaults to None.
    - days (int): Number of past days to fetch records from, based on last
      updated date. Defaults to 1.

    Returns:
    - orders (list): List of order records from Shopify.

    Iterates over Shopify API response pages, fetching records in batches.
    Continues requests until all orders fetched, response_limit reached,
    or no more pages left.

    If `days` is set, fetches records updated within last `days`. For each API
    request, prints order count and accessed URL for monitoring.

    Raises:
    - HTTPError: If API request fails.
    '''
    today = date.today()
    orders = []
    params = {'limit': batch_limit}
    if days:
        start_date = today - timedelta(days=days)
        print('[Start execution] Get Shopify orders '
              f'from {start_date} to {today}')
        params['updated_at_min'] = \
            (datetime.now() - timedelta(days=days)).isoformat()
    else:
        print('[Start execution] Get Shopify orders all dates')
    url = urljoin(SHOPIFY_API_URL, 'orders.json?status=any')

    requests_count = 0
    while url:
        try:
            print(f'Requests count: {requests_count}')
            'Shopify API Limitations'
            time.sleep(1) if requests_count % 20 == 0 else 0
            response = requests.get(
                url,
                params=params,
                auth=HTTPBasicAuth(SHOPIFY_API_KEY, SHOPIFY_API_PASSWORD)
            )
            requests_count += 1

            'Raises an HTTPError if the request returned an error status code'
            response.raise_for_status()
            orders.extend(response.json()['orders'])
            if len(orders) >= batch_size:
                print(f'To process {len(orders)} orders')
                process_orders(orders)
                orders = []

            if response_limit and len(orders) >= response_limit:
                print('Response limit')
                print(f'Processing the last batch of {len(orders)} orders')
                process_orders(orders) if orders else 0
                orders = []
                break

            '''Extracts the 'Link' header from the response headers. This
            header contains URLs for pagination (next page, previous page).'''
            link_header = response.headers.get('Link')

            if link_header:
                links = link_header.split(', ')

                url = None
                for link in links:
                    if 'rel="next"' in link:
                        url = link[link.index('<')+1:link.index('>')]
                        params = None
                        break
            else:
                print('No more link header')
                print(f'Processing the last batch of {len(orders)} orders')
                process_orders(orders) if orders else 0
                orders = []
                url = None
        except (HTTPError, ConnectionError) as e:
            print(f'Error encountered: {e}')
            max_retries -= 1
            if max_retries > 0:
                print('Waiting 2 seconds before retrying...')
                time.sleep(2)
                continue
            else:
                print('Max retries exceeded.')
                break

    if orders:
        print(f'Processing the last batch of {len(orders)} orders')
        process_orders(orders)
    return orders


def orders_to_dataframe(orders_datalist):
    '''
    Converts a list of order data into a Pandas DataFrame.

    Parameters:
    - orders_datalist (list): A list of dictionaries, each representing an
      order's data.

    Returns:
    - DataFrame: A Pandas DataFrame containing the order data,
        with appropriately named columns.
    '''
    if orders_datalist:
        
        orders_cleaned = []
        shipping_addresses = []
        orders_line = []
        for order in orders_datalist:
            customer_info = order.get('customer') or {}
            shipping_address = order.get('shipping_address') or {}
            order_data = {
                'ORDER_ID': order.get('id'),
                'EMAIL': order.get('email') or order.get('contact_email'),
                'CREATED_AT': order.get('created_at'),
                'CURRENT_SUBTOTAL_PRICE': order.get('current_subtotal_price'),
                'CURRENT_TOTAL_DISCOUNTS':
                    order.get('current_total_discounts'),
                'CURRENT_TOTAL_PRICE': order.get('current_total_price'),
                'FINANCIAL_STATUS': order.get('financial_status'),
                'NAME': order.get('name'),
                'PROCESSED_AT': order.get('processed_at'),
                'SUBTOTAL_PRICE': order.get('subtotal_price'),
                'UPDATED_AT': order.get('updated_at'),
                'CUSTOMER_ID': customer_info.get('id'),
                'SMS_MARKETING_CONSENT':
                    customer_info.get('sms_marketing_consent'),
                'TAGS': customer_info.get('tags'),
                'ACCEPTS_MARKETING': customer_info.get('accepts_marketing'),
                'ACCEPTS_MARKETING_UPDATED_AT':
                    customer_info.get('accepts_marketing_updated_at'),
                'MARKETING_OPT_IN_LEVEL':
                    customer_info.get('marketing_opt_in_level'),
                'DISCOUNTED_PRICE': order.get(
                    'shipping_lines', [{}])[0].get('discounted_price')
                if order.get('shipping_lines') else None,
            }
            orders_cleaned.append(order_data)

            shipping_data = {
                'ORDER_ID': order.get('id'),
                'CUSTOMER_ID': shipping_address.get('company'),
                'EMAIL': order.get('email') or order.get('contact_email'),
                'ORDER_DATE': order.get('created_at'),
                'FIRST_NAME': shipping_address.get('first_name'),
                'LAST_NAME': shipping_address.get('last_name'),
                'ADDRESS1': shipping_address.get('address1'),
                'ADDRESS2': shipping_address.get('address2'),
                'CITY': shipping_address.get('city'),
                'ZIP': shipping_address.get('zip'),
                'PROVINCE': shipping_address.get('province'),
                'COUNTRY': shipping_address.get('country'),
                'PHONE': shipping_address.get('phone'),
                'LATITUDE': shipping_address.get('latitude'),
                'LONGITUDE': shipping_address.get('longitude'),
                'ACCEPTS_MARKETING': customer_info.get('accepts_marketing'),
                'MARKETING_OPT_IN_LEVEL':
                    customer_info.get('marketing_opt_in_level'),
            }
            shipping_addresses.append(shipping_data)

            line_items = order.get('line_items')

            for line in line_items:
                order_line_data = {
                    'ORDER_ID': order.get('id'),
                    'LINE_ITEM_ID': line.get('id'),
                    'ORDER_NAME': order.get('name'),
                    'SKU': line.get('sku'),
                    'QUANTITY': line.get('quantity'),
                 }
                orders_line.append(order_line_data)  # Move this inside the loop

        orders_df = pd.DataFrame(orders_cleaned)
        shipping_df = pd.DataFrame(shipping_addresses)
        orders_line_df = pd.DataFrame(orders_line)

        print(orders_line_df.head(20).to_string())
        print(f'Creating/updating {len(orders_datalist)} orders from Shopify.')
        return orders_df, shipping_df, orders_line_df
    else:
        print('No data received from get_shopify_orders')
        return None


def run_get_shopify_orders(**context):
    '''
    A wrapper function that chains fetching order data from Shopify,
    transforming it into a DataFrame, and subsequently writing it into
    Snowflake.

    This function calls `get_shopify_orders` to fetch order data
    from Shopify, transforms the fetched data into a DataFrame using
    `orders_to_dataframe`, and writes the DataFrame to Snowflake
    using the `write_data_to_snowflake` function.
    '''
    execution_date = context['execution_date']
    print(f'Execution Date: {execution_date}')
    get_shopify_orders(
        batch_limit=BATCH_LIMIT,
        response_limit=RESPONSE_LIMIT,
        days=DAYS,
        batch_size=BATCH_SIZE
    )


def process_orders(orders_list):
    orders_dataframe, shipping_addresses_dataframe, orders_line_dataframe= \
        orders_to_dataframe(orders_list)

    print(orders_dataframe.head().to_string())
    print(shipping_addresses_dataframe.head().to_string())
    print(orders_line_dataframe.head().to_string())

    write_data_to_snowflake(
        orders_line_dataframe,
        'SHOPIFY_ORDERS_LINE',
        default_args['snowflake_shopify_orders_line_table_columns'],
        'LINE_ITEM_ID',
        'TEMP_SHOPIFY_ORDERS_LINE',
        SNOWFLAKE_CONN_ID
        )


# Task definitions
task_1 = PythonOperator(
    task_id='get_shopify_orders',
    python_callable=run_get_shopify_orders,
    dag=dag,
)
