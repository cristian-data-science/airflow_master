from datetime import timedelta, datetime, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from dags.config.shopify_customer_data_config import default_args
from dags.utils.utils import write_data_to_snowflake, fetch_data_from_snowflake
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin
import os
import requests
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
def get_shopify_customers(batch_limit=250, response_limit=None, days=1):
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
    start_date = today - timedelta(days=days)
    print(f'[Start execution] Get Shopify customers '
          f'from {start_date} to {today}')
    customers = []
    params = {'limit': batch_limit}
    if days:
        params['updated_at_min'] = \
            (datetime.now() - timedelta(days=days)).isoformat()
    url = urljoin(SHOPIFY_API_URL, 'customers.json')

    while url:
        # Make a GET request to the Shopify API using the provided URL.
        # The request includes parameters for pagination and authentication.
        response = requests.get(
            url,
            params=params,
            auth=HTTPBasicAuth(SHOPIFY_API_KEY, SHOPIFY_API_PASSWORD)
        )

        # Raises an HTTPError if the request returned an error status code.
        response.raise_for_status()

        # Append the fetched customers to the customers list. The response is
        # expected to be in JSON format with a key 'customers'.
        customers.extend(response.json()['customers'])

        # If a response limit is set and the number of fetched customers
        # reaches or exceeds this limit, break out of the loop.
        if response_limit and len(customers) >= response_limit:
            break

        # Extracts the 'Link' header from the response headers. This header
        # contains URLs for pagination (next page, previous page).
        link_header = response.headers.get('Link')

        if link_header:
            # Split the Link header to extract individual links.
            links = link_header.split(', ')

            # Reset URL to ensure fresh assignment from the Link header.
            url = None

            for link in links:
                # Check for the presence of a 'next' relation type in the Link.
                if 'rel="next"' in link:
                    # Extract the URL for the next page of results.
                    url = link[link.index('<')+1:link.index('>')]

                    # Clear the params for the next request, as the 'next' URL
                    # already contains the required parameters.
                    params = None
                    break
        else:
            # If there is no 'Link' header in the response, set URL to None to
            # stop further pagination.
            url = None

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
    customers_datalist = \
        get_shopify_customers(batch_limit=200, response_limit=None, days=3)
    customers_dataframe = \
        customers_to_dataframe(customers_datalist)
    write_data_to_snowflake(
        customers_dataframe,
        'SHOPIFY_CUSTOMERS',
        default_args['snowflake_shopify_customer_table_columns'],
        'SHOPIFY_ID',
        'TEMP_SHOPIFY_CUSTOMERS',
        SNOWFLAKE_CONN_ID
    )


def get_shopify_customer_addresses(customer_ids):
    addresses = []
    for index, customer_id in enumerate(customer_ids, start=1):
        print(f'[Shopify] Get addresses for customer '
              f'{index} - ID: {customer_id}')
        url = urljoin(
            SHOPIFY_API_URL,
            f'customers/{customer_id}/addresses.json'
            )
        response = requests.get(
            url,
            auth=HTTPBasicAuth(SHOPIFY_API_KEY, SHOPIFY_API_PASSWORD)
        )
        response.raise_for_status()
        customer_addresses = response.json().get('addresses', [])
        addresses.extend([
            address for address in customer_addresses
        ])
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


def run_get_shopify_customer_addresses(start_date=None, end_date=None):
    end_date = date.today()
    start_date = end_date - timedelta(days=3)
    customer_ids = fetch_data_from_snowflake(
        'SHOPIFY_CUSTOMERS', 'SHOPIFY_ID', 'UPDATED_AT',
        start_date, end_date, SNOWFLAKE_CONN_ID
    )
    print(f'[Snowflake] Modified customers between {start_date} '
          f'anf {end_date}: {len(customer_ids)}')
    addresses = get_shopify_customer_addresses(customer_ids)
    addresses_df = addresses_to_dataframe(addresses)

    write_data_to_snowflake(
        addresses_df,
        'SHOPIFY_CUSTOMER_ADDRESSES',
        default_args['snowflake_shopify_customer_addresses_table_columns'],
        'SHOPIFY_ID',
        'TEMP_SHOPIFY_CUSTOMER_ADDRESSES',
        SNOWFLAKE_CONN_ID
    )


# Task definitions
task_1 = PythonOperator(
    task_id='get_shopify_customers',
    python_callable=run_get_shopify_customers,
    dag=dag,
)

# Task definitions
task_2 = PythonOperator(
    task_id='get_shopify_customer_addresses',
    python_callable=run_get_shopify_customer_addresses,
    dag=dag,
)

task_1 >> task_2
