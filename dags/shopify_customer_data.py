from datetime import timedelta, datetime, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from dags.config.shopify_customer_data_config import default_args
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

# Dag definition
dag = DAG(
    'shopify_customer_data',
    default_args=default_args,
    description='DAG to extract customer data from Shopify and ERP and '
    'consolidate it in a single table in Snowflake',
    schedule_interval=timedelta(days=1),
)


# Tasks functions
def get_shopify_customers(
        batch_limit=250, response_limit=None,
        days=1, batch_size=10000, max_retries=5
        ):
    '''
    Fetches customer data from Shopify API with pagination support and filters
    based on the last updated date.

    Parameters:
    - batch_limit (int): Max number of records per API call. Defaults to 250.
    - response_limit (int, optional): Max total records to fetch. If None,
      fetches all. Defaults to None.
    - days (int): Number of past days to fetch records from, based on last
      updated date. Defaults to 1.

    Returns:
    - customers (list): List of customer records from Shopify.

    Iterates over Shopify API response pages, fetching records in batches.
    Continues requests until all customers fetched, response_limit reached,
    or no more pages left.

    If `days` is set, fetches records updated within last `days`. For each API
    request, prints customer count and accessed URL for monitoring.

    Raises:
    - HTTPError: If API request fails.
    '''
    today = date.today()
    customers = []
    params = {'limit': batch_limit}
    if days:
        start_date = today - timedelta(days=days)
        print('[Start execution] Get Shopify customers '
              f'from {start_date} to {today}')
        params['updated_at_min'] = \
            (datetime.now() - timedelta(days=days)).isoformat()
    else:
        print('[Start execution] Get Shopify customers all dates')
    url = urljoin(SHOPIFY_API_URL, 'customers.json')

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
            customers.extend(response.json()['customers'])
            if len(customers) >= batch_size:
                print(f'To process {len(customers)} customers')
                process_customers(customers)
                customers = []

            if response_limit and len(customers) >= response_limit:
                print('Response limit')
                print(f'Processing the last batch of {len(customers)} '
                      'customers')
                process_customers(customers) if customers else 0
                customers = []
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
                print(f'Processing the last batch of {len(customers)} '
                      'customers')
                process_customers(customers) if customers else 0
                customers = []
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

    if customers:
        print(f'Processing the last batch of {len(customers)} customers')
        process_customers(customers)
    return customers


def customers_to_dataframe(customers_datalist):
    '''
    Converts a list of customer data into a Pandas DataFrame.

    This function is useful for transforming data retrieved from an API
    into a structured format.

    Parameters:
    - customers_datalist (list): A list of dictionaries, each representing
        a customer's data.

    Returns:
    - DataFrame: A Pandas DataFrame containing the customer data,
        with appropriately named columns.

    The function first checks if `customers_datalist` is not empty.
    It then converts the list of dictionaries into a Pandas DataFrame,
    renames the columns to match the specified column names, and reorders
    the columns according to a predefined order. It prints the first few
    rows of the DataFrame for review and returns the processed DataFrame.
    '''
    if customers_datalist is not None:
        for customer in customers_datalist:
            consent = customer.get('email_marketing_consent', {})
            customer['accepts_marketing'] = \
                consent.get('state', None)
            customer['accepts_marketing_updated_at'] = \
                consent.get('consent_updated_at', None)
            customer['marketing_opt_in_level'] = \
                consent.get('opt_in_level', None)
        df = pd.DataFrame(customers_datalist)
        df = df.rename(columns={
            'id': 'SHOPIFY_ID',
            'email': 'EMAIL',
            'accepts_marketing': 'ACCEPTS_MARKETING',
            'created_at': 'CREATED_AT',
            'updated_at': 'UPDATED_AT',
            'first_name': 'FIRST_NAME',
            'last_name': 'LAST_NAME',
            'orders_count': 'ORDERS_COUNT',
            'state': 'STATE',
            'total_spent': 'TOTAL_SPENT',
            'last_order_id': 'LAST_ORDER_ID',
            'note': 'NOTE',
            'verified_email': 'VERIFIED_EMAIL',
            'multipass_identifier': 'MULTIPASS_IDENTIFIER',
            'tax_exempt': 'TAX_EXEMPT',
            'tags': 'TAGS',
            'last_order_name': 'LAST_ORDER_NAME',
            'currency': 'CURRENCY',
            'phone': 'PHONE',
            'accepts_marketing_updated_at': 'ACCEPTS_MARKETING_UPDATED_AT',
            'marketing_opt_in_level': 'MARKETING_OPT_IN_LEVEL',
            'admin_graphql_api_id': 'ADMIN_GRAPHQL_API_ID',
        })
        df = df[['SHOPIFY_ID', 'EMAIL', 'ACCEPTS_MARKETING', 'CREATED_AT',
                 'UPDATED_AT', 'FIRST_NAME', 'LAST_NAME', 'ORDERS_COUNT',
                 'STATE', 'TOTAL_SPENT', 'LAST_ORDER_ID', 'NOTE',
                 'VERIFIED_EMAIL', 'MULTIPASS_IDENTIFIER', 'TAX_EXEMPT',
                 'TAGS', 'LAST_ORDER_NAME', 'CURRENCY', 'PHONE',
                 'ACCEPTS_MARKETING_UPDATED_AT', 'MARKETING_OPT_IN_LEVEL',
                 'ADMIN_GRAPHQL_API_ID']]
        print(df.head().to_string())
        print(f'Creating/updating {len(customers_datalist)} '
              'customers from Shopify.')
        print(df.shape)
        return df
    else:
        print('No data received from get_shopify_customers')
        return None


def run_get_shopify_customers(**context):
    '''
    A wrapper function that chains fetching customer data from Shopify,
    transforming it into a DataFrame, and subsequently writing it into
    Snowflake.

    This function calls `get_shopify_customers` to fetch customer data
    from Shopify, transforms the fetched data into a DataFrame using
    `customers_to_dataframe`, and writes the DataFrame to Snowflake
    using the `write_data_to_snowflake` function.
    '''
    execution_date = context['execution_date']
    print(f'Execution Date: {execution_date}')
    get_shopify_customers(
        batch_limit=250, response_limit=None, days=3, batch_size=10000
    )


def process_customers(customers_list):
    customers_dataframe = \
        customers_to_dataframe(customers_list)
    addresses = get_shopify_customer_addresses(customers_list)
    addresses_df = addresses_to_dataframe(addresses)
    most_repeated_values_df = get_most_repeated_values_dataframe(addresses_df)

    customers_merged_df = pd.merge(
        customers_dataframe,
        most_repeated_values_df,
        on="SHOPIFY_ID",
        how="left"
    )

    write_data_to_snowflake(
        customers_merged_df,
        'SHOPIFY_CUSTOMERS',
        default_args['snowflake_shopify_customer_table_columns'],
        ['SHOPIFY_ID'],
        'TEMP_SHOPIFY_CUSTOMERS',
        SNOWFLAKE_CONN_ID
    )

    write_data_to_snowflake(
        addresses_df,
        'SHOPIFY_CUSTOMER_ADDRESSES',
        default_args['snowflake_shopify_customer_addresses_table_columns'],
        ['SHOPIFY_ID'],
        'TEMP_SHOPIFY_CUSTOMER_ADDRESSES',
        SNOWFLAKE_CONN_ID
    )


def get_shopify_customer_addresses(customers_list):
    addresses = []
    for customer in customers_list:
        addresses.extend(customer.get('addresses', []))

    return addresses


def addresses_to_dataframe(addresses_datalist):
    if addresses_datalist is not None:
        df = pd.DataFrame(addresses_datalist)
        df = df.rename(columns={
            'id': 'SHOPIFY_ID',
            'customer_id': 'CUSTOMER_ID',
            'first_name': 'FIRST_NAME',
            'last_name': 'LAST_NAME',
            'company': 'COMPANY',
            'address1': 'ADDRESS1',
            'address2': 'ADDRESS2',
            'city': 'CITY',
            'province': 'PROVINCE',
            'country': 'COUNTRY',
            'zip': 'ZIP',
            'phone': 'PHONE',
            'name': 'NAME',
            'province_code': 'PROVINCE_CODE',
            'country_code': 'COUNTRY_CODE',
            'country_name': 'COUNTRY_NAME',
            'default': 'DEFAULT'
        })
        print(df.head().to_string())
        print(f'Creating/updating {len(addresses_datalist)} '
              'customers addresses from Shopify.')
        print(df.shape)
        return df
    else:
        print('No data received from get_shopify_customer_addresses')
        return None


def get_most_repeated_values_dataframe(addresses_df):
    'Insert most repeat values '
    if not addresses_df.empty:
        most_repeated_values = addresses_df.groupby('CUSTOMER_ID').agg({
            'NAME': lambda x:
                x.mode().get(0, None) if not x.empty else None,
            'PHONE': lambda x:
                x.mode().get(0, None) if not x.empty else None,
            'ADDRESS1': lambda x:
                x.mode().get(0, None) if not x.empty else None,
            'ADDRESS2': lambda x:
                x.mode().get(0, None) if not x.empty else None,
            'COMPANY': lambda x:
                x.mode().get(0, None) if not x.empty else None,
            'PROVINCE': lambda x:
                x.mode().get(0, None) if not x.empty else None,
            'CITY': lambda x:
                x.mode().get(0, None) if not x.empty else None,
                }).reset_index().rename(columns={
                    'CUSTOMER_ID': 'SHOPIFY_ID',
                    'NAME': 'MOST_REPEATED_NAME',
                    'PHONE': 'MOST_REPEATED_PHONE',
                    'ADDRESS1': 'MOST_REPEATED_ADDRESS1',
                    'ADDRESS2': 'MOST_REPEATED_ADDRESS2',
                    'COMPANY': 'MOST_REPEATED_RUT',
                    'PROVINCE': 'MOST_REPEATED_PROVINCE',
                    'CITY': 'MOST_REPEATED_CITY'
                })
    return most_repeated_values


# Task definitions
task_1 = PythonOperator(
    task_id='get_shopify_customers',
    python_callable=run_get_shopify_customers,
    dag=dag,
)
