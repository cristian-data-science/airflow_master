from datetime import timedelta, datetime, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from config.shopify_order_data_config import default_args
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

BATCH_LIMIT = 250
RESPONSE_LIMIT = None
DAYS = 2
BATCH_SIZE = 10000

# DAG definition
dag = DAG(
    'shopify_orders_data',
    default_args=default_args,
    description='DAG to extract order data from Shopify using GraphQL '
                'and write in Snowflake',
    schedule_interval='0 9,17 * * *',
    catchup=False,
    tags=['shopify', 'orders']
)


def strip_gid_prefix(gid_str, obj_type='Order'):
    """
    Strip 'gid://shopify/Order/' or similar prefixes.
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
    Build dynamically the GraphQL query to fetch orders from Shopify.

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
      orders(first: {first}{after_part}{query_part}, sortKey: UPDATED_AT) {{
        edges {{
          node {{
            id
            name
            email
            createdAt
            updatedAt
            processedAt
            displayFinancialStatus
            subtotalPriceSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
            totalDiscountsSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
            totalPriceSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
            currentSubtotalPriceSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
            currentTotalDiscountsSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
            currentTotalPriceSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
            customer {{
              id
              email
              emailMarketingConsent {{
                marketingState
                marketingOptInLevel
                consentUpdatedAt
              }}
              tags
            }}
            shippingAddress {{
              firstName
              lastName
              company
              address1
              address2
              city
              province
              provinceCode
              country
              countryCode
              zip
              phone
              latitude
              longitude
            }}
            lineItems(first: 50) {{
              edges {{
                node {{
                  id
                  name
                  quantity
                  sku
                  originalUnitPriceSet {{
                    shopMoney {{
                      amount
                      currencyCode
                    }}
                  }}
                  discountedUnitPriceSet {{
                    shopMoney {{
                      amount
                      currencyCode
                    }}
                  }}
                }}
              }}
            }}
            shippingLines(first: 5) {{
              edges {{
                node {{
                  id
                  title
                  discountedPriceSet {{
                    shopMoney {{
                      amount
                      currencyCode
                    }}
                  }}
                }}
              }}
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


def get_shopify_orders(
        batch_limit=250,
        response_limit=None,
        days=1,
        batch_size=10000,
        max_retries=5
):
    """
    Fetches order data from Shopify using GraphQL API.

    :param batch_limit: Number of records to fetch per request.
    :param response_limit: Maximum number of records to fetch.
    :param days: Number of days to fetch data from.
    :param batch_size: Number of records to process in each batch.
    :param max_retries: Maximum number of retries in case of error.
    :return: List of dictionaries with order data.
    """
    today = date.today()
    orders = []

    # Si se desea filtrar por fecha
    if days:
        start_date = today - timedelta(days=days)
        print('[Start execution] Get Shopify orders '
              f'from {start_date} to {today}')
        updated_at_filter = (datetime.now() - timedelta(days=days)).isoformat()
    else:
        print('[Start execution] Get Shopify orders all dates')
        updated_at_filter = None

    url = SHOPIFY_GRAPHQL_ENDPOINT

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
            orders_data = data['data']['orders']
            edges = orders_data['edges']

            for edge in edges:
                node = edge['node']

                # Clean order ID
                raw_order_id = node.get('id')
                clean_order_id = strip_gid_prefix(raw_order_id, 'Order')

                # Clean customer ID if exists
                customer_info = node.get('customer', {})
                if customer_info and 'id' in customer_info:
                    raw_customer_id = customer_info['id']
                    customer_info['id'] = \
                        strip_gid_prefix(raw_customer_id, 'Customer')

                # Process line items
                line_items = []
                if 'lineItems' in node and 'edges' in node['lineItems']:
                    for line_edge in node['lineItems']['edges']:
                        line_node = line_edge['node']

                        # Clean line item ID
                        raw_line_id = line_node.get('id')
                        clean_line_id = \
                            strip_gid_prefix(raw_line_id, 'LineItem')

                        line_item = {
                            'id': clean_line_id,
                            'name': line_node.get('name'),
                            'quantity': line_node.get('quantity'),
                            'sku': line_node.get('sku'),
                            'original_price': line_node.get(
                                'originalUnitPriceSet',
                                {}).get('shopMoney', {}).get('amount'),
                            'discounted_price': line_node.get(
                                'discountedUnitPriceSet',
                                {}).get('shopMoney', {}).get('amount'),
                        }
                        line_items.append(line_item)

                # Process shipping lines
                shipping_discount = None
                if ('shippingLines' in node and
                        'edges' in node['shippingLines'] and
                        node['shippingLines']['edges']):
                    # Get first shipping line
                    shipping_edge = node['shippingLines']['edges'][0]
                    shipping_node = shipping_edge['node']
                    if ('discountedPriceSet' in shipping_node and
                            'shopMoney' in
                            shipping_node['discountedPriceSet']):
                        shipping_discount = (
                            shipping_node['discountedPriceSet']['shopMoney']
                            .get('amount'))

                # Create order dictionary
                order_dict = {
                    'id': clean_order_id,
                    'name': node.get('name'),
                    'email': node.get('email'),
                    'created_at': node.get('createdAt'),
                    'updated_at': node.get('updatedAt'),
                    'processed_at': node.get('processedAt'),
                    'financial_status': node.get('displayFinancialStatus'),
                    'subtotal_price': (
                        node.get('subtotalPriceSet', {})
                        .get('shopMoney', {}).get('amount')
                    ),
                    'total_discounts': (
                        node.get('totalDiscountsSet', {})
                        .get('shopMoney', {}).get('amount')
                    ),
                    'total_price': (
                        node.get('totalPriceSet', {})
                        .get('shopMoney', {}).get('amount')
                    ),
                    'current_subtotal_price': (
                        node.get('currentSubtotalPriceSet', {})
                        .get('shopMoney', {}).get('amount')
                    ),
                    'current_total_discounts': (
                        node.get('currentTotalDiscountsSet', {})
                        .get('shopMoney', {}).get('amount')
                    ),
                    'current_total_price': (
                        node.get('currentTotalPriceSet', {})
                        .get('shopMoney', {}).get('amount')
                    ),
                    'customer': customer_info,
                    'shipping_address': node.get('shippingAddress'),
                    'line_items': line_items,
                    'discounted_shipping_price': shipping_discount
                }

                orders.append(order_dict)

            if len(orders) >= batch_size:
                print(f'To process {len(orders)} orders')
                process_orders(orders)
                orders = []

            # Verificamos si se ha llegado al límite total de registros
            if response_limit and len(orders) >= response_limit:
                print('Response limit')
                print(f'Processing the last batch of {len(orders)} orders')
                if orders:
                    process_orders(orders)
                orders = []
                break

            page_info = orders_data['pageInfo']
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
            email_marketing_consent = (
                customer_info.get('emailMarketingConsent') or {})
            marketing_state = email_marketing_consent.get('marketingState')
            marketing_opt_in_level = (
                email_marketing_consent.get('marketingOptInLevel', ''))
            consent_updated_at = (
                email_marketing_consent.get('consentUpdatedAt'))

            order_data = {
                'ORDER_ID': order.get('id'),
                'EMAIL': order.get('email'),
                'CREATED_AT': order.get('created_at'),
                'CURRENT_SUBTOTAL_PRICE': order.get('current_subtotal_price'),
                'CURRENT_TOTAL_DISCOUNTS': (
                    order.get('current_total_discounts')
                ),
                'CURRENT_TOTAL_PRICE': order.get('current_total_price'),
                'FINANCIAL_STATUS':
                    (order.get('financial_status') or '').lower(),
                'NAME': order.get('name'),
                'PROCESSED_AT': order.get('processed_at'),
                'SUBTOTAL_PRICE': order.get('subtotal_price'),
                'UPDATED_AT': order.get('updated_at'),
                'CUSTOMER_ID': customer_info.get('id'),
                'SMS_MARKETING_CONSENT': None,
                'TAGS': customer_info.get('tags'),
                'ACCEPTS_MARKETING': marketing_state == 'SUBSCRIBED',
                'ACCEPTS_MARKETING_UPDATED_AT': consent_updated_at,
                'MARKETING_OPT_IN_LEVEL': marketing_opt_in_level,
                'DISCOUNTED_PRICE': order.get('discounted_shipping_price'),
            }
            orders_cleaned.append(order_data)

            shipping_data = {
                'ORDER_ID': order.get('id'),
                'CUSTOMER_ID': shipping_address.get('company'),
                'EMAIL': order.get('email'),
                'ORDER_DATE': order.get('created_at'),
                'FIRST_NAME': shipping_address.get('firstName'),
                'LAST_NAME': shipping_address.get('lastName'),
                'ADDRESS1': shipping_address.get('address1'),
                'ADDRESS2': shipping_address.get('address2'),
                'CITY': shipping_address.get('city'),
                'ZIP': shipping_address.get('zip'),
                'PROVINCE': shipping_address.get('province'),
                'COUNTRY': shipping_address.get('country'),
                'PHONE': shipping_address.get('phone'),
                'LATITUDE': shipping_address.get('latitude'),
                'LONGITUDE': shipping_address.get('longitude'),
                'ACCEPTS_MARKETING': marketing_state == 'SUBSCRIBED',
                'MARKETING_OPT_IN_LEVEL': marketing_opt_in_level,
            }
            shipping_addresses.append(shipping_data)

            line_items = order.get('line_items', [])

            for line in line_items:
                order_line_data = {
                    'ORDER_ID': order.get('id'),
                    'LINE_ITEM_ID': line.get('id'),
                    'CREATED_AT': order.get('created_at'),
                    'ORDER_NAME': order.get('name'),
                    'SKU': line.get('sku'),
                    'QUANTITY': line.get('quantity'),
                }
                orders_line.append(order_line_data)

        orders_df = pd.DataFrame(orders_cleaned)
        shipping_df = pd.DataFrame(shipping_addresses)
        orders_line_df = pd.DataFrame(orders_line)

        print(orders_df.head().to_string())
        print(
            f'Creating/updating {len(orders_datalist)} orders from `Shopify.')
        print(orders_line_df.head().to_string())
        print(
            f'Creating/updating {orders_line_df.shape[0]} '
            'orders from Shopify.'
        )

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
    orders_dataframe, shipping_addresses_dataframe, orders_line_dataframe = \
        orders_to_dataframe(orders_list)

    print(orders_dataframe.head().to_string())
    print(shipping_addresses_dataframe.head().to_string())
    print(orders_line_dataframe.head().to_string())

    write_data_to_snowflake(
        orders_dataframe,
        'SHOPIFY_ORDERS',
        default_args['snowflake_shopify_order_table_columns'],
        ['ORDER_ID'],
        'TEMP_SHOPIFY_ORDERS',
        SNOWFLAKE_CONN_ID
    )

    write_data_to_snowflake(
        shipping_addresses_dataframe,
        'SHOPIFY_ORDERS_SHIPPING_ADDRESSES',
        default_args['snowflake_shopify_shipping_address_table_columns'],
        ['ORDER_ID'],
        'TEMP_SHOPIFY_ORDERS_SHIPPING_ADDRESSES',
        SNOWFLAKE_CONN_ID
    )

    write_data_to_snowflake(
        orders_line_dataframe,
        'SHOPIFY_ORDERS_LINE',
        default_args['snowflake_shopify_orders_line_table_columns'],
        ['LINE_ITEM_ID'],
        'TEMP_SHOPIFY_ORDERS_LINE',
        SNOWFLAKE_CONN_ID
    )


# Task definitions
task_1 = PythonOperator(
    task_id='get_shopify_orders',
    python_callable=run_get_shopify_orders,
    provide_context=True,
    dag=dag,
)
