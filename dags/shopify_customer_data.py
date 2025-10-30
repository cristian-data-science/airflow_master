from datetime import timedelta, datetime, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from config.shopify_customer_data_config import default_args
from utils.utils import write_data_to_snowflake
import os
import requests
from requests.exceptions import HTTPError, ConnectionError
import time
import pandas as pd

# Load environment variables from .env file
load_dotenv()
SHOPIFY_API_KEY = os.getenv('SHOPIFY_API_KEY')
SHOPIFY_API_PASSWORD = os.getenv('SHOPIFY_API_PASSWORD')
SHOPIFY_API_URL = os.getenv('SHOPIFY_API_URL')
SHOPIFY_API_VERSION = os.getenv('SHOPIFY_API_VERSION')
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')
SHOPIFY_GRAPHQL_ENDPOINT = \
    SHOPIFY_API_URL + SHOPIFY_API_VERSION + '/graphql.json'

# DAG definition
dag = DAG(
    'shopify_customer_data',
    default_args=default_args,
    description='DAG to extract customer data from Shopify (GraphQL) and ERP '
                'and consolidate it in a single table in Snowflake',
    schedule_interval='0 5 * * *',
    catchup=False,
    tags=['shopify', 'customer']
)


def strip_gid_prefix(gid_str, obj_type='Customer'):
    """
    Strip 'gid://shopify/Customer/' or 'MailingAddress/' etc.
    Return just the numeric portion or last segment.
    """
    if not gid_str:
        return gid_str
    prefix = f"gid://shopify/{obj_type}/"
    if gid_str.startswith(prefix):
        return gid_str.split('/')[-1]
    return gid_str


def build_graphql_query(first, after=None, updated_at_min=None):
    """
    Build dynaically the GraphQL query to fetch customers from Shopify.

    :param first: Number of records to fetch.
    :param after: Cursor to paginate the results.
    :param updated_at_min: Filter to fetch only records updated
        after this date.
    :return: GraphQL query string.
    """
    after_part = f', after: "{after}"' if after else ''
    query_part = \
        f', query: "updated_at:>={updated_at_min}"' if updated_at_min else ''

    return f"""
    {{
      customers(first: {first}{after_part}{query_part}) {{
        edges {{
          node {{
            id
            email
            firstName
            lastName
            numberOfOrders
            amountSpent {{
              amount
              currencyCode
            }}
            createdAt
            updatedAt
            emailMarketingConsent {{
              marketingState
              marketingOptInLevel
              consentUpdatedAt
            }}
            addresses {{
                id
                firstName
                lastName
                address1
                address2
                company
                phone
                city
                province
                country
                zip
                name
                provinceCode
                countryCode
            }}
          }}
        }}
        pageInfo {{
          hasNextPage
          endCursor
        }}
      }}
    }}
    """


def get_shopify_customers(
        batch_limit=250,
        response_limit=None,
        days=1,
        batch_size=10000,
        max_retries=5
):
    """
    Fetches customer data from Shopify using GraphQL API.

    :param batch_limit: Number of records to fetch per request.
    :param response_limit: Maximum number of records to fetch.
    :param days: Number of days to fetch data from.
    :param batch_size: Number of records to process in each batch.
    :param max_retries: Maximum number of retries in case of error.
    :return: List of dictionaries with customer data.
    """
    today = date.today()
    customers = []

    url = SHOPIFY_GRAPHQL_ENDPOINT

    # Log de versión de API
    print(f'[Shopify API] Using API version: {SHOPIFY_API_VERSION}')
    print(f'[Shopify API] Endpoint: {SHOPIFY_GRAPHQL_ENDPOINT}')

    # Si se desea filtrar por fecha
    if days:
        start_date = today - timedelta(days=days)
        print('[Start execution] Get Shopify customers '
              f'from {start_date} to {today}')
        updated_at_filter = (datetime.now() - timedelta(days=days)).isoformat()
    else:
        print('[Start execution] Get Shopify customers all dates')
        updated_at_filter = None

    requests_count = 0
    after_cursor = None
    has_next_page = True

    while has_next_page:
        try:
            print(f'Requests count: {requests_count}')
            time.sleep(1) if requests_count % 20 == 0 else None

            query = build_graphql_query(
                first=batch_limit,
                after=after_cursor,
                updated_at_min=updated_at_filter
            )

            # POST request to GraphQL
            response = requests.post(
                url,
                json={"query": query},
                headers={
                    "Content-Type": "application/json",
                    "X-Shopify-Access-Token": SHOPIFY_API_PASSWORD
                }
            )
            requests_count += 1
            response.raise_for_status()

            data = response.json()
            customers_data = data['data']['customers']
            edges = customers_data['edges']

            for edge in edges:
                node = edge['node']

                raw_customer_id = node.get('id')
                clean_customer_id = \
                    strip_gid_prefix(raw_customer_id, 'Customer')

                addresses_list = node.get('addresses', [])
                for addr in addresses_list:
                    addr_id = addr.get('id')
                    addr['id'] = strip_gid_prefix(addr_id, 'MailingAddress')
                    if '?model_name=CustomerAddress' in addr['id']:
                        addr['id'] = addr['id'].split('?')[0]
                    addr['customer_id'] = clean_customer_id

                customer_dict = {
                    'id': clean_customer_id,
                    'email': node.get('email'),
                    'first_name': node.get('firstName'),
                    'last_name': node.get('lastName'),
                    'orders_count': node.get('numberOfOrders', 0),

                    'total_spent': (node['amountSpent']['amount']
                                    if node.get('amountSpent') else 0),
                    'currency': (node['amountSpent']['currencyCode']
                                 if node.get('amountSpent') else None),

                    'created_at': node.get('createdAt'),
                    'updated_at': node.get('updatedAt'),

                    'email_marketing_consent': {
                        'state': (node
                                  ['emailMarketingConsent']['marketingState']
                                  if node.get('emailMarketingConsent')
                                  else None),
                        'opt_in_level': (node
                                         ['emailMarketingConsent']
                                         ['marketingOptInLevel']
                                         if node.get('emailMarketingConsent')
                                         else None),
                        'consent_updated_at': (node['emailMarketingConsent']
                                               ['consentUpdatedAt']
                                               if node.get(
                                                   'emailMarketingConsent')
                                               else None)
                    },
                    'addresses': addresses_list,
                    'note': None,
                    'last_order_id': None,
                    'last_order_name': None,
                    'phone': None,
                    'admin_graphql_api_id': None,
                    'accepts_marketing_updated_at': None,
                    'marketing_opt_in_level': None,
                    'state': None,
                    'verified_email': None,
                    'multipass_identifier': None,
                    'tax_exempt': None,
                    'tags': None,
                }
                customers.append(customer_dict)

            if len(customers) >= batch_size:
                print(f'To process {len(customers)} customers')
                process_customers(customers)
                customers = []

            # Verificamos si se ha llegado al límite total de registros
            if response_limit and len(customers) >= response_limit:
                print('Response limit')
                print(f'Processing the last batch of {len(customers)}'
                      ' customers')
                if customers:
                    process_customers(customers)
                customers = []
                break

            page_info = customers_data['pageInfo']
            has_next_page = page_info['hasNextPage']
            after_cursor = page_info['endCursor'] if has_next_page else None

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
    """
    Convert the list of dictionaries (customers) into a DataFrame.
    """
    if customers_datalist is not None:
        for customer in customers_datalist:
            consent = customer.get('email_marketing_consent', {})
            customer['accepts_marketing'] = consent.get('state', None)
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
        df = df[[
            'SHOPIFY_ID', 'EMAIL', 'ACCEPTS_MARKETING', 'CREATED_AT',
            'UPDATED_AT', 'FIRST_NAME', 'LAST_NAME', 'ORDERS_COUNT', 'STATE',
            'TOTAL_SPENT', 'LAST_ORDER_ID', 'NOTE', 'VERIFIED_EMAIL',
            'MULTIPASS_IDENTIFIER', 'TAX_EXEMPT', 'TAGS', 'LAST_ORDER_NAME',
            'CURRENCY', 'PHONE', 'ACCEPTS_MARKETING_UPDATED_AT',
            'MARKETING_OPT_IN_LEVEL', 'ADMIN_GRAPHQL_API_ID'
        ]]

        print(df.head().to_string())
        print(f'Creating/updating {len(customers_datalist)} customers '
              'from Shopify.')
        print(df.shape)
        return df
    else:
        print('No data received from get_shopify_customers')
        return None


def get_shopify_customer_addresses(customers_list):
    """
    Extract the addresses from the customers list.
    """
    addresses = []
    for customer in customers_list:
        addresses.extend(customer.get('addresses', []))
    return addresses


def addresses_to_dataframe(addresses_datalist):
    """
    Convert the list of dictionaries (addresses) into a DataFrame.
    """
    if addresses_datalist is not None:
        for addr in addresses_datalist:
            if 'company' not in addr:
                addr['company'] = None
            if 'name' not in addr:
                addr['name'] = None
            if 'default' not in addr:
                addr['default'] = None
        df = pd.DataFrame(addresses_datalist)
        df = df.rename(columns={
            'id': 'SHOPIFY_ID',
            'customer_id': 'CUSTOMER_ID',
            'firstName': 'FIRST_NAME',
            'lastName': 'LAST_NAME',
            'company': 'COMPANY',
            'address1': 'ADDRESS1',
            'address2': 'ADDRESS2',
            'city': 'CITY',
            'province': 'PROVINCE',
            'country': 'COUNTRY',
            'zip': 'ZIP',
            'phone': 'PHONE',
            'name': 'NAME',
            'provinceCode': 'PROVINCE_CODE',
            'countryCode': 'COUNTRY_CODE',
            'default': 'DEFAULT'
        })
        df['COUNTRY_NAME'] = df['COUNTRY']
        print(df.head().to_string())
        print(f'Creating/updating {len(addresses_datalist)} customers '
              'addresses from Shopify.')
        print(df.shape)
        return df
    else:
        print('No data received from get_shopify_customer_addresses')
        return None


def get_most_repeated_values_dataframe(addresses_df):
    """
    Logic to get the most repeated values for each customer.
    """
    if not addresses_df.empty:
        most_repeated_values = addresses_df.groupby('CUSTOMER_ID').agg({
            'NAME': lambda x: x.mode().get(0, None) if not x.empty else None,
            'PHONE': lambda x: x.mode().get(0, None) if not x.empty else None,
            'ADDRESS1': lambda x: x.mode().get(0, None)
            if not x.empty else None,
            'ADDRESS2': lambda x: x.mode().get(0, None)
            if not x.empty else None,
            'COMPANY': lambda x: x.mode().get(0, None)
            if not x.empty else None,
            'PROVINCE': lambda x: x.mode().get(0, None)
            if not x.empty else None,
            'CITY': lambda x: x.mode().get(0, None) if not x.empty else None,
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
    else:
        return pd.DataFrame(columns=[
            'SHOPIFY_ID', 'MOST_REPEATED_NAME', 'MOST_REPEATED_PHONE',
            'MOST_REPEATED_ADDRESS1', 'MOST_REPEATED_ADDRESS2',
            'MOST_REPEATED_RUT', 'MOST_REPEATED_PROVINCE', 'MOST_REPEATED_CITY'
        ])


def process_customers(customers_list):
    """
    Takes the customers list and processes it to write it to Snowflake.
    """
    customers_dataframe = customers_to_dataframe(customers_list)
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


def run_get_shopify_customers(**context):
    """
    Function to run the Shopify customer data extraction.
    """
    execution_date = context['execution_date']
    print(f'Execution Date: {execution_date}')
    get_shopify_customers(
        batch_limit=250,
        response_limit=None,
        days=3,
        batch_size=10000
    )


# Task definitions
task_1 = PythonOperator(
    task_id='get_shopify_customers',
    python_callable=run_get_shopify_customers,
    dag=dag,
)
