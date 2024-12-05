import os
import requests
from pandas import DataFrame
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from config.shopify_product_variants_data_config import default_args
from utils.utils import write_data_to_snowflake


load_dotenv()
SHOPIFY_API_KEY = os.getenv('SHOPIFY_API_KEY')
SHOPIFY_API_PASSWORD = os.getenv('SHOPIFY_API_PASSWORD')
SHOPIFY_API_URL = \
    os.getenv('SHOPIFY_API_URL') + os.getenv('SHOPIFY_API_VERSION') + '/'
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')


dag = DAG(
    'shopify_product_variants',
    default_args=default_args,
    description='DAG to extract product variants data '
    'from Shopify and consolidate it in Snowflake',
    schedule_interval='0 11 * * *',
    catchup=False,
)


def get_shopify_product_variants(response_limit=None, days=None):
    '''
    Fetches product variants from Shopify API, with optional filtering based
    on the number of days since last update and an optional response size
    limit.

    Parameters:
    - response_limit (int, optional): Max number of variants to fetch.
      Fetches all if None. Defaults to None.
    - days (int, optional): Number of days back from today to filter updates.
      Fetches all if None. Defaults to None.

    Constructs a query to Shopify API, optionally filtering by 'updated_at'
    to include only recently updated products. Paginates through Shopify API
    responses, accumulating variants until reaching the specified limit or
    no more products are available.

    Returns:
    - variants (list of dict): List of dictionaries, each representing a
      product variant, including details like ID, title, price, inventory
      quantity, and more.
    '''
    print('[Start execution] Get Shopify product variants')
    products_with_variants = []
    url = SHOPIFY_API_URL + 'products.json'
    params = {'limit': 250}

    if days:
        updated_at_min = datetime.utcnow() - timedelta(days=days)
        params['updated_at_min'] = updated_at_min.strftime('%Y-%m-%dT%H:%M:%S')

    while url:
        print(f'[API Shopify] Request for URL {url} - params: {params}')
        response = requests.get(
            url,
            params=params,
            auth=(SHOPIFY_API_KEY, SHOPIFY_API_PASSWORD)
        )
        response.raise_for_status()
        products = response.json()['products']
        print(f'[API Shopify] {len(products)} products.')
        print(products)
        for product in products:
            if response_limit and len(
                    products_with_variants) >= response_limit:
                break
            products_with_variants.append(product)
            if response_limit and len(
                    products_with_variants) >= response_limit:
                break

        if response_limit and len(products_with_variants) >= response_limit:
            break

        link_header = response.headers.get('Link')
        if link_header and 'rel="next"' in link_header:
            links = link_header.split(', ')
            url = None
            for link in links:
                if 'rel="next"' in link:
                    url = link[link.index('<')+1:link.index('>')]
                    params = None
                    break
        else:
            url = None

    return products_with_variants


def variants_to_dataframe(products_list):
    '''
    Converts a list of Shopify product and variant data into a Pandas
    DataFrame.

    Parameters:
    - products_list (list): A list of dictionaries, each representing
    the information of a product and its variants.

    Returns:
    - DataFrame: A Pandas DataFrame containing the data of the products
    and their variants, with appropriately named columns.
    '''
    print('[AIRFLOW] Dataframe processing started')
    transformed_data = []
    for product in products_list:
        for variant in product['variants']:
            # Initialize image variables
            image_id, image_alt, image_src = None, None, None

            # Find the corresponding image for the variant
            for image in product['images']:
                if variant['id'] in image['variant_ids']:
                    image_id = image['id']
                    image_alt = image['alt']
                    image_src = image['src']
                    break

            transformed_data.append({
                'PRODUCT_ID': product['id'],
                'VARIANT_ID': variant['id'],
                'PRODUCT_TITLE': product['title'],
                'PRODUCT_BODY_HTML': product['body_html'],
                'PRODUCT_VENDOR': product['vendor'],
                'PRODUCT_TYPE': product['product_type'],
                'PRODUCT_CREATED_AT': product['created_at'],
                'PRODUCT_HANDLE': product['handle'],
                'PRODUCT_UPDATED_AT': product['updated_at'],
                'PRODUCT_PUBLISHED_AT': product['published_at'],
                'PRODUCT_PUBLISHED_SCOPE': product['published_scope'],
                'PRODUCT_TAGS': product['tags'],
                'PRODUCT_STATUS': product['status'],
                'VARIANT_TITLE': variant['title'],
                'VARIANT_SKU': variant['sku'],
                'VARIANT_PRICE': variant['price'],
                'VARIANT_COMPARE_AT_PRICE': variant.get(
                    'compare_at_price', None),
                'VARIANT_INVENTORY_ITEM_ID': variant.get(
                    'inventory_item_id', None),
                'VARIANT_INVENTORY_QUANTITY': variant.get(
                    'inventory_quantity', None),
                'VARIANT_INVENTORY_POLICY': variant[
                    'inventory_policy'],
                'VARIANT_INVENTORY_MANAGEMENT': variant[
                    'inventory_management'],
                'VARIANT_OPTION1': variant['option1'],
                'VARIANT_OPTION2': variant['option2'],
                'VARIANT_OPTION3': variant.get('option3', None),
                'VARIANT_CREATED_AT': variant['created_at'],
                'VARIANT_UPDATED_AT': variant['updated_at'],
                'VARIANT_TAXABLE': variant['taxable'],
                'VARIANT_BARCODE': variant['barcode'],
                'VARIANT_WEIGHT': variant['weight'],
                'VARIANT_WEIGHT_UNIT': variant['weight_unit'],
                'VARIANT_REQUIRES_SHIPPING': variant['requires_shipping'],
                'IMAGE_ID': image_id,
                'IMAGE_ALT': image_alt,
                'IMAGE_SRC': image_src
            })
    df = DataFrame(transformed_data)
    print(df.head().to_string())
    print(f'Creating/updating {len(transformed_data)} '
          'product variants from Shopify.')
    return df


def run_get_shopify_product_variants(**context):
    variants_list = get_shopify_product_variants(
        response_limit=None, days=10
    )
    variants_df = variants_to_dataframe(variants_list)
    if variants_df is not None:
        write_data_to_snowflake(
            variants_df,
            'SHOPIFY_PRODUCT_VARIANTS2',
            default_args['snowflake_shopify_product_variants_table_columns'],
            ['VARIANT_ID'],
            'TEMP_SHOPIFY_PRODUCT_VARIANTS2',
            SNOWFLAKE_CONN_ID
        )


task_get_variants = PythonOperator(
    task_id='get_shopify_product_variants',
    python_callable=run_get_shopify_product_variants,
    dag=dag,
)
