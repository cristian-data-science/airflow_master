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
SHOPIFY_API_URL = os.getenv('SHOPIFY_API_URL')
SHOPIFY_API_VERSION = os.getenv('SHOPIFY_API_VERSION')
SHOPIFY_GRAPHQL_ENDPOINT = \
    SHOPIFY_API_URL + SHOPIFY_API_VERSION + '/graphql.json'
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')


dag = DAG(
    'shopify_product_variants',
    default_args=default_args,
    description='DAG to extract product variants data '
    'from Shopify and consolidate it in Snowflake',
    schedule_interval='0 11 * * *',
    catchup=False,
)


def build_graphql_query(first, after=None, updated_at_min=None):
    '''
    Build dynamically the GraphQL query to fetch products and their variants
    from Shopify.

    :param first: Number of records to fetch.
    :param after: Cursor to paginate the results.
    :param updated_at_min: Filter to fetch only records updated
    after this date.
    :return: GraphQL query string.
    '''
    query = '''
    query ($first: Int!, $after: String, $updatedAt: String) {
      products(first: $first, after: $after, query: $updatedAt) {
        pageInfo {
          hasNextPage
          endCursor
        }
        edges {
          node {
            id
            title
            bodyHtml
            vendor
            productType
            createdAt
            handle
            updatedAt
            publishedAt
            tags
            status

            # Product images
            images(first: 10) {
              edges {
                node {
                  id
                  altText
                  url
                }
              }
            }

            # Product variants
            variants(first: 10) {
              edges {
                node {
                  id
                  title
                  sku
                  price
                  compareAtPrice

                  # Inventory by location
                  inventoryItem {
                    id
                    inventoryLevels(first: 1) {
                      edges {
                        node {
                          location { id name }
                          quantities(names: ["available"]) {
                            name
                            quantity
                          }
                        }
                      }
                    }
                  }

                  inventoryPolicy
                  inventoryManagement
                  selectedOptions { name value }
                  createdAt
                  updatedAt
                  taxable
                  barcode
                  weight
                  weightUnit
                  requiresShipping

                  # Variant image
                  image {
                    id
                    altText
                    url
                  }
                }
              }
            }
          }
        }
      }
    }
    '''

    variables = {
        'first': first
    }

    if after:
        variables['after'] = after

    if updated_at_min:
        variables['updatedAt'] = f'updated_at:>={updated_at_min}'

    return query, variables


def strip_gid_prefix(gid_str, obj_type='Product'):
    '''
    Strip 'gid://shopify/Product/' or similar prefixes.
    Return just the numeric portion or last segment.
    '''
    if not gid_str:
        return None
    prefix = f'gid://shopify/{obj_type}/'
    if gid_str.startswith(prefix):
        return gid_str[len(prefix):]
    return gid_str


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
    batch_limit = 250
    has_next_page = True
    after_cursor = None

    updated_at_min = None
    if days:
        updated_at_min = (datetime.utcnow() - timedelta(days=days))
        updated_at_min = updated_at_min.strftime('%Y-%m-%dT%H:%M:%S')

    while has_next_page:
        query, variables = build_graphql_query(
            first=batch_limit,
            after=after_cursor,
            updated_at_min=updated_at_min
        )

        print(f'[API Shopify] GraphQL request with cursor: {after_cursor}')
        response = requests.post(
            SHOPIFY_GRAPHQL_ENDPOINT,
            json={'query': query, 'variables': variables},
            auth=(SHOPIFY_API_KEY, SHOPIFY_API_PASSWORD)
        )
        response.raise_for_status()
        data = response.json()

        if 'errors' in data:
            print('GraphQL Errors:', data['errors'])
            break

        products_data = data['data']['products']
        products = [edge['node'] for edge in products_data['edges']]
        print(f'[API Shopify] {len(products)} products.')

        for product in products:
            if (response_limit and
                    len(products_with_variants) >= response_limit):
                break

            # Transform GraphQL response to match REST API structure
            transformed_product = {
                'id': strip_gid_prefix(product['id']),
                'title': product['title'],
                'body_html': product['bodyHtml'],
                'vendor': product['vendor'],
                'product_type': product['productType'],
                'created_at': product['createdAt'],
                'handle': product['handle'],
                'updated_at': product['updatedAt'],
                'published_at': product['publishedAt'],

                'tags': (', '.join(product['tags'])
                         if isinstance(product['tags'], list)
                         else product['tags']),
                'status': product['status'].lower(),
                'variants': [],
                'images': []
            }

            # Transform variants
            for variant_edge in product['variants']['edges']:
                variant = variant_edge['node']
                # Get inventory quantity
                inventory_levels = \
                    variant['inventoryItem']['inventoryLevels']['edges']
                inventory_quantity = 0
                if inventory_levels:
                    quantities = inventory_levels[0]['node']['quantities']
                    available_qty = next(
                        (q['quantity'] for q in quantities
                         if q['name'] == 'available'), 0)
                    inventory_quantity = available_qty

                transformed_variant = {
                    'id': strip_gid_prefix(variant['id'], 'ProductVariant'),
                    'title': variant['title'],
                    'sku': variant['sku'],
                    'price': variant['price'],
                    'compare_at_price': variant['compareAtPrice'],
                    'inventory_item_id': strip_gid_prefix(
                        variant['inventoryItem']['id'], 'InventoryItem'),
                    'inventory_quantity': inventory_quantity,
                    'inventory_policy': variant['inventoryPolicy'].lower(),
                    'inventory_management': (
                        variant['inventoryManagement'].lower()),
                    'option1': next(
                        (opt['value']
                         for opt in variant['selectedOptions']
                         if (opt['name'].lower() == 'option1' or
                             opt['name'].lower() == 'title')),
                        None),
                    'option2': next(
                        (opt['value']
                         for opt in variant['selectedOptions']
                         if (opt['name'].lower() == 'option2' or
                             opt['name'].lower() == 'size')),
                        None),
                    'option3': next(
                        (opt['value']
                         for opt in variant['selectedOptions']
                         if (opt['name'].lower() == 'option3' or
                             opt['name'].lower() == 'color')),
                        None),
                    'created_at': variant['createdAt'],
                    'updated_at': variant['updatedAt'],
                    'taxable': variant['taxable'],
                    'barcode': variant['barcode'],
                    'weight': variant['weight'],
                    'weight_unit': (
                        'g' if variant['weightUnit'].lower() == 'grams'
                        else variant['weightUnit'].lower()
                    ),
                    'requires_shipping': variant['requiresShipping']
                }
                transformed_product['variants'].append(transformed_variant)

            # Transform images
            for image_edge in product['images']['edges']:
                image = image_edge['node']
                transformed_image = {
                    'id': strip_gid_prefix(image['id'], 'Image'),
                    'alt': image['altText'],
                    'src': image['url']
                }
                transformed_product['images'].append(transformed_image)

            products_with_variants.append(transformed_product)

        if response_limit and len(products_with_variants) >= response_limit:
            break

        page_info = products_data['pageInfo']
        has_next_page = page_info['hasNextPage']
        after_cursor = page_info['endCursor'] if has_next_page else None

    return products_with_variants


def ensure_numeric_id(id_value):
    '''
    Ensure that an ID is numeric, stripping any GraphQL prefixes if needed.

    Parameters:
    - id_value: The ID value to process

    Returns:
    - A numeric ID or None if the input is invalid
    '''
    if id_value is None:
        return None

    # If it's already a number, return it
    if isinstance(id_value, (int, float)):
        return id_value

    # If it's a string with gid prefix, strip it
    if isinstance(id_value, str):
        if id_value.startswith('gid://shopify/'):
            # Extract the numeric part after the last slash
            try:
                return int(id_value.split('/')[-1])
            except (ValueError, IndexError):
                return None
        # Try to convert to int if it's a numeric string
        try:
            return int(id_value)
        except ValueError:
            return None

    return None


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

            # Check if variant has an associated image
            if 'image' in variant and variant['image']:
                image_id = strip_gid_prefix(variant['image']['id'], 'Image')
                image_alt = variant['image']['altText']
                image_src = variant['image']['url']
            elif product['images']:
                image = product['images'][0]
                image_id = strip_gid_prefix(
                    image.get('id'), 'Image')
                image_alt = image.get('alt')
                image_src = image.get('src')

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
                'PRODUCT_PUBLISHED_SCOPE': None,
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
                'IMAGE_ID': ensure_numeric_id(image_id),
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
