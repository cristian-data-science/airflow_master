import os
import requests
import json
import pytz
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import timedelta, datetime, timezone
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from dags.config.klaviyo_retail_sales_loading_config import default_args


load_dotenv()
SHOPIFY_API_KEY = os.getenv('SHOPIFY_API_KEY')
SHOPIFY_API_PASSWORD = os.getenv('SHOPIFY_API_PASSWORD')
SHOPIFY_API_URL = \
    os.getenv('SHOPIFY_API_URL') + os.getenv('SHOPIFY_API_VERSION') + '/'
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')
KLAVIYO_API_URL = os.getenv('KLAVIYO_API_URL')
KLAVIYO_API_TOKEN = os.getenv('KLAVIYO_API_TOKEN')
# KLAVIYO_DATE = "2024-06-14 05:38:00-04:00"

START_DATE = '2024-03-30'
END_DATE = '2024-03-30'
CUSTOMER_ACCOUNT = None  # None to process all customers
AVOID_RUTS = ['55555555-5', '66666666-6', '22222222-2']
AVOID_EMAILS = ['poschile@patagonia.com', 'patagonia@patagonia.com']

customer_updated_count = 0
processed_sales = set()
processed_customers = set()
email_found_in_processed_sale = set()
email_found_in_erp_customers_primary_contact = set()
email_found_in_erp_customers_receipt_mail = set()
email_found_in_shopify = set()
email_not_found = set()
klaviyo_existing_customers = set()
klaviyo_new_customers = set()


dag = DAG(
    'klaviyo_retail_sales_loading',
    default_args=default_args,
    description='DAG to load retail sales data '
    'from Snowflake and load it in Klaviyo',
    schedule_interval=timedelta(days=1),
    catchup=False
)


def get_retail_sales(
        start_date, end_date, cust_account=None):
    '''
    Retrieves retail sales data from Snowflake for a given date range.

    Parameters:
    - start_date (str): Start date in 'YYYY-MM-DD' format.
    - end_date (str): End date in 'YYYY-MM-DD' format.
    - snowflake_conn_id (str): Airflow connection ID for Snowflake.
    - cust_account (str, optional): Customer account to filter by.
        Defaults to None.

    Returns:
    - pandas.DataFrame: DataFrame containing the retail sales data.
    '''
    query = f"""
    WITH latest_shopify_address AS (
        SELECT CUSTOMER_ID, EMAIL
        FROM (
            SELECT CUSTOMER_ID, EMAIL,
                   ROW_NUMBER() OVER (
                    PARTITION BY CUSTOMER_ID ORDER BY ORDER_ID DESC
                    ) AS rn
            FROM SHOPIFY_ORDERS_SHIPPING_ADDRESSES
        )
        WHERE rn = 1
    )
    SELECT sl.*,
           ec.PRIMARYCONTACTEMAIL AS ERP_CUST_PRIMARYCONTACTEMAIL,
           ec.RECEIPTEMAIL,
           so.EMAIL AS SHOPIFY_EMAIL
    FROM ERP_PROCESSED_SALESLINE sl
    LEFT JOIN ERP_CUSTOMERS ec
        ON sl.CUSTACCOUNT = ec.CUSTOMERACCOUNT
    LEFT JOIN latest_shopify_address so
        ON sl.CUSTACCOUNT = so.CUSTOMER_ID
    WHERE DATE(sl.INVOICEDATE)
            BETWEEN '{start_date}' AND '{end_date}'
        AND sl.CECO = 'Retail'
    """

    if cust_account:
        query += f" AND CUSTACCOUNT = '{cust_account}'"
    else:
        avoid_ruts_str = ', '.join([f"'{rut}'" for rut in AVOID_RUTS])
        query += f" AND CUSTACCOUNT NOT IN ({avoid_ruts_str})"

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()

    df = pd.DataFrame(result, columns=columns)

    # Convert CREATEDTRANSACTIONDATE2 from UTC to local time
    df['CREATEDTRANSACTIONDATE2'] = \
        pd.to_datetime(df['CREATEDTRANSACTIONDATE2'], utc=True)
    local_tz = pytz.timezone('America/Santiago')
    df['CREATEDTRANSACTIONDATE2'] = \
        df['CREATEDTRANSACTIONDATE2'
           ].apply(lambda x: x.tz_convert(local_tz).isoformat())
    print(f'[Airflow] Fetched {df.shape[0]} sale lines from Snowflake.')
    print(f'{df["SALESID"].nunique()} Sales.')
    total_customer_to_process = df["CUSTACCOUNT"].nunique()
    print(f'{total_customer_to_process} Customers.')
    return df


def check_klaviyo_profile(email):
    url = f"{KLAVIYO_API_URL}/profiles/?filter=equals(email,\"{email}\")"
    headers = {
        'Authorization': f'Klaviyo-API-Key {KLAVIYO_API_TOKEN}',
        'revision': '2023-12-15',
        'Accept': 'application/json'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        if data['data']:
            profile = data['data'][0]
            accepts_marketing = profile[
                'attributes'
                ].get('properties', {}).get('Accepts Marketing', False)
            return True, accepts_marketing
        else:
            return False, None

    except requests.RequestException as e:
        print(f"Error al consultar la API de Klaviyo: {e}")
        return False, None


def get_customer_email_from_erp(cust_account):
    query = f"""
    SELECT PRIMARYCONTACTEMAIL, RECEIPTEMAIL
    FROM ERP_CUSTOMERS
    WHERE CUSTOMERACCOUNT = '{cust_account}'
    """

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    conn.close()

    if result:
        primary_email, receipt_email = result
        return primary_email if primary_email else receipt_email
    return None


def get_customer_email_from_shopify(cust_account):
    shopify_query = f"""
    SELECT EMAIL
    FROM SHOPIFY_ORDERS_SHIPPING_ADDRESSES
    WHERE CUSTOMER_ID = '{cust_account}'
    ORDER BY ORDER_DATE DESC
    LIMIT 1
    """

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(shopify_query)
    shopify_result = cursor.fetchone()
    cursor.close()
    conn.close()

    if shopify_result:
        return shopify_result[0]

    return None


def process_retail_sales_df(retail_sales_df):
    customers_dict = {}

    for _, row in retail_sales_df.iterrows():
        cust_account = row['CUSTACCOUNT']
        if cust_account in AVOID_RUTS:
            print(f'[Airflow] Omitting customer with RUT: {cust_account}')
            continue

        sales_id = row['SALESID']
        customer_email = row['PRIMARYCONTACTEMAIL']

        if customer_email:
            print(f'[Airflow] Mail found for PRIMARYCONTACTEMAIL in '
                  f'sale {sales_id}|{customer_email}')
            email_found_in_processed_sale.add(cust_account)
        else:
            customer_email = row['ERP_CUST_PRIMARYCONTACTEMAIL']
            if customer_email:
                print(f'[Airflow] Mail found for ERP_CUST_PRIMARYCONTACTEMAIL'
                      f' in ERP_CUSTOMERS in sale {sales_id}|{customer_email}')
                email_found_in_erp_customers_primary_contact.add(cust_account)
            else:
                customer_email = row['RECEIPTEMAIL']
                if customer_email:
                    print(f'[Airflow] Mail found for RECEIPTEMAIL customer in '
                          f'ERP_CUSTOMERS in sale {sales_id}|{customer_email}')
                    email_found_in_erp_customers_receipt_mail.add(cust_account)
                else:
                    customer_email = row['SHOPIFY_EMAIL']
                    if customer_email:
                        print(f'[Airflow] Mail found for EMAIL in '
                              f'SHOPIFY_ORDERS_SHIPPING in sale {sales_id}|'
                              f'{customer_email}')
                        email_found_in_shopify.add(cust_account)
                    else:
                        print(f'[Airflow] No mail found for customer in '
                              f'any table for sale '
                              f'{sales_id}|{cust_account}')
                        email_not_found.add(cust_account)

        if customer_email and customer_email not in AVOID_EMAILS:
            if customer_email not in customers_dict:
                customers_dict[customer_email] = {
                    'customer_email': customer_email,
                    'customer_RUT': cust_account,
                    'full_name': row['ORGANIZATIONNAME'],
                    'orders': {}
                }
                processed_customers.add(customer_email)

            if sales_id not in customers_dict[customer_email]['orders']:
                customers_dict[customer_email]['orders'][sales_id] = {
                    'lines': [],
                    'retail_store': row['CANAL'],
                    'transaction_date': row['CREATEDTRANSACTIONDATE2'],
                    'invoice_id': row['INVOICEID']
                }
                processed_sales.add(sales_id)

            customers_dict[customer_email]['orders'][sales_id]['lines'].append(
                {
                    'sku': (f"{row['ITEMID']}-"
                            f"{row['INVENTCOLORID']}-{row['INVENTSIZEID']}"),
                    'quantity': row['QTY'],
                    'price_with_taxes': row['LINEAMOUNTWITHTAXES']
                }
            )
        else:
            print(f'[Airflow] No mail found for customer '
                  f'in SHOPIFY_ORDERS_SHIPPING ADD in sale {sales_id}')

    return customers_dict


def check_skus_in_shopify(customers_dict):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    skus = set()
    for customer in customers_dict.values():
        for order in customer['orders'].values():
            for line in order['lines']:
                skus.add(line['sku'])

    skus = list(skus)
    print('## SKUs to check ##')
    print(skus)

    format_strings = ','.join(['%s'] * len(skus))
    query = f"""
    SELECT VARIANT_SKU, PRODUCT_ID, VARIANT_ID,
        PRODUCT_TITLE, PRODUCT_TYPE, PRODUCT_HANDLE,
        PRODUCT_TAGS, IMAGE_ID, IMAGE_ALT, IMAGE_SRC
    FROM SHOPIFY_PRODUCT_VARIANTS2
    WHERE VARIANT_SKU IN ({format_strings})
    """
    cursor.execute(query, tuple(skus))
    sku_info = {
        row[0]: {
                'PRODUCT_ID': row[1],
                'VARIANT_ID': row[2],
                'PRODUCT_TITLE': row[3],
                'PRODUCT_TYPE': row[4],
                'PRODUCT_HANDLE': row[5],
                'PRODUCT_TAGS': row[6],
                'IMAGE_ID': row[7],
                'IMAGE_ALT': row[8],
                'IMAGE_SRC': row[9],
                }
        for row in cursor.fetchall()
    }
    cursor.close()
    conn.close()

    for customer in customers_dict.values():
        for order in customer['orders'].values():
            for line in order['lines']:
                sku = line['sku']
                if sku in sku_info:
                    line.update(sku_info[sku])
                else:
                    line.update({
                        'PRODUCT_ID': None,
                        'VARIANT_ID': None,
                        'PRODUCT_TITLE': None,
                        'PRODUCT_TYPE': None,
                        'PRODUCT_HANDLE': None,
                        'PRODUCT_TAGS': None,
                        'IMAGE_ID': None,
                        'IMAGE_ALT': None,
                        'IMAGE_SRC': None
                    })

    return customers_dict


def create_klaviyo_json_list(customer, show_location_info=False):
    print(
        f'[Airflow] Processing orders for customer: {customer["full_name"]}'
    )
    print(f'Customer email: {customer["customer_email"]}')
    print(f'{len(customer["orders"])} Orders: {customer["orders"].keys()}')

    jsons_list = []
    for sale_id, sale in customer['orders'].items():
        print(f'[Airflow] Sale: {sale_id} in {sale["retail_store"]} '
              f'at {sale["transaction_date"]}')
        transaction_date = \
            datetime.fromisoformat(
                sale['transaction_date']
                ).astimezone(timezone.utc).isoformat()
        event_name = (
                "Placed Retail Order"
                if not show_location_info
                else f"Placed Retail Order - "
                f"{str(sale['retail_store']).upper()}"
            )
        if sale['invoice_id'].startswith('61-'):
            event_name = "[NC] Placed retail return"
            if show_location_info:
                event_name += f" - {sale['retail_store'].upper()}"

        all_skus_have_variant_id = \
            all(line.get('VARIANT_ID') is not None
                for line in sale['lines'])
        if all_skus_have_variant_id:
            print(f'[Airflow] All SKUs have a variant ID in sale {sale_id}. '
                  'Shopify json format')
            items = [line['PRODUCT_TITLE'] for line in sale['lines']]
            total_price = sum(
                    line['price_with_taxes']
                    for line in sale['lines'])
            line_items = []
            product_tags = set()
            for line in sale['lines']:
                product_tags.update(
                    line['PRODUCT_TAGS'].split(', ')
                    if line['PRODUCT_TAGS'] else []
                )
                line_items.append({
                    "id": line['VARIANT_ID'],
                    "fulfillable_quantity": line['quantity'],
                    "fulfillment_service": "manual",
                    "fulfillment_status": "fulfilled",
                    "gift_card": False,
                    "grams": 200,
                    "name": line['PRODUCT_TITLE'],
                    "price": line['price_with_taxes'],
                    "price_set": {
                        "shop_money": {
                            "amount": line['price_with_taxes'],
                            "currency_code": "CLP"
                        },
                        "presentment_money": {
                            "amount": line['price_with_taxes'],
                            "currency_code": "CLP"
                        }
                    },
                    "product_exists": True,
                    "product_id": line['PRODUCT_ID'],
                    "quantity": line['quantity'],
                    "requires_shipping": True,
                    "sku": line['sku'],
                    "taxable": False,
                    "title": line['PRODUCT_TITLE'],
                    "total_discount": "0",
                    "variant_id": line['VARIANT_ID'],
                    "variant_inventory_management": "shopify",
                    "vendor": "Patagonia",
                    "line_price": line['price_with_taxes'],
                    "product": {
                        "id": line['PRODUCT_ID'],
                        "title": line['PRODUCT_TITLE'],
                        "handle": line['PRODUCT_HANDLE'],
                        "vendor": "Patagonia",
                        "product_type": line['PRODUCT_TYPE'],
                        "images": [
                            {
                                "src": line['IMAGE_SRC'],
                                "thumb_src": line['IMAGE_SRC'],
                                "alt": line['IMAGE_ALT'],
                                "width": 2000,
                                "height": 2000,
                                "position": 1,
                                "id": line['IMAGE_ID']
                            }
                        ],
                        "variant": {
                            "sku": line['sku'],
                            "images": [
                                {
                                    "src": line['IMAGE_SRC'],
                                    "thumb_src": line['IMAGE_SRC'],
                                    "alt": line['IMAGE_ALT'],
                                    "width": 2000,
                                    "height": 2000,
                                    "position": 1,
                                    "id": line['IMAGE_ID']
                                }
                            ]
                        }
                    }
                })

            name_parts = customer['full_name'].split(' ', 1)
            first_name = name_parts[0]
            last_name = name_parts[1] if len(name_parts) > 1 else ''

            event_json = {
                "token": KLAVIYO_API_TOKEN,
                "event": event_name,
                "customer_properties": {
                    "$first_name": first_name,
                    "$last_name": last_name,
                    "$email": customer['customer_email'],
                    "$organization": customer['customer_RUT'],
                    "Accepts Marketing": customer['accepts_marketing']

                },
                "time": transaction_date,
                "properties": {
                    "Items": items,
                    "Collections": list(product_tags),
                    "Item Count": len(sale['lines']),
                    "tags": [],
                    "OptedInToSmsOrderUpdates": False,
                    "Discount Codes": [],
                    "Total Discounts": "0",
                    "Order ID": sale_id,
                    "Location": sale['retail_store'],
                    "Source Name": "retail",
                    "extra": {
                        "id": sale_id,
                        "buyer_accepts_marketing": customer[
                            'accepts_marketing'],
                        "company": None,
                        "confirmation_number": "",
                        "contact_email": customer['customer_email'],
                        "currency": "CLP",
                        "current_subtotal_price": total_price,
                        "current_subtotal_price_set": {
                            "shop_money": {
                                "amount": total_price,
                                "currency_code": "CLP"
                            },
                            "presentment_money": {
                                "amount": total_price,
                                "currency_code": "CLP"
                            }
                        },
                        "current_total_additional_fees_set": None,
                        "current_total_discounts": "0",
                        "current_total_discounts_set": {
                            "shop_money": {
                                "amount": "0",
                                "currency_code": "CLP"
                            },
                            "presentment_money": {
                                "amount": "0",
                                "currency_code": "CLP"
                            }
                        },
                        "current_total_duties_set": None,
                        "current_total_price": total_price,
                        "current_total_price_set": {
                            "shop_money": {
                                "amount": total_price,
                                "currency_code": "CLP"
                            },
                            "presentment_money": {
                                "amount": total_price,
                                "currency_code": "CLP"
                            }
                        },
                        "current_total_tax": "0",
                        "customer_locale": "es-CL",
                        "discount_codes": [],
                        "email": customer['customer_email'],
                        "estimated_taxes": False,
                        "financial_status": "paid",
                        "fulfillment_status": "fulfilled",
                        "location_id": sale['retail_store'],
                        "merchant_of_record_app_id": None,
                        "name": sale_id,
                        "note": sale['retail_store'],
                        "note_attributes": [],
                        "number": sale_id,
                        "order_number": sale_id,
                        "original_total_additional_fees_set": None,
                        "original_total_duties_set": None,
                        "payment_gateway_names": [
                            "Checkout Mercado Pago"
                        ],
                        "phone": None,
                        "presentment_currency": "CLP",
                        "processed_at": sale['transaction_date'],
                        "subtotal_price": total_price,
                        "subtotal_price_set": {
                            "shop_money": {
                                "amount": total_price,
                                "currency_code": "CLP"
                            },
                            "presentment_money": {
                                "amount": total_price,
                                "currency_code": "CLP"
                            }
                        },
                        "tags": "",
                        "tax_exempt": False,
                        "tax_lines": [],
                        "taxes_included": True,
                        "total_discounts": "0",
                        "total_discounts_set": {
                            "shop_money": {
                                "amount": "0",
                                "currency_code": "CLP"
                            },
                            "presentment_money": {
                                "amount": "0",
                                "currency_code": "CLP"
                            }
                        },
                        "total_line_items_price": total_price,
                        "total_line_items_price_set": {
                            "shop_money": {
                                "amount": total_price,
                                "currency_code": "CLP"
                            },
                            "presentment_money": {
                                "amount": total_price,
                                "currency_code": "CLP"
                            }
                        },
                        "total_outstanding": "0",
                        "total_price": total_price,
                        "total_price_set": {
                            "shop_money": {
                                "amount": total_price,
                                "currency_code": "CLP"
                            },
                            "presentment_money": {
                                "amount": total_price,
                                "currency_code": "CLP"
                            }
                        },
                        "total_tip_received": "0",
                        "total_weight": 200,
                        "updated_at": sale['transaction_date'],
                        "user_id": None,
                        "discount_applications": [],
                        "line_items": line_items
                    },
                    "$value": total_price,
                    "$location": sale['retail_store'],
                }
            }

            jsons_list.append(event_json)

        else:
            print(f'[Airflow] Not all SKUs have a variant ID. Sale {sale_id}')
            print('[Airflow] Creating custom JSON format')
            failed_skus = \
                [i['sku'] for i in sale['lines'] if i['VARIANT_ID'] is None]
            print('Failed Skus: ', failed_skus)
            product_tags = set(
                tag
                for line in sale['lines']
                for tag in (
                    line['PRODUCT_TAGS'].split(', ')
                    if line['PRODUCT_TAGS'] else [])
            )
            event_json = {
                "token": KLAVIYO_API_TOKEN,
                "event": event_name,
                "customer_properties": {
                    "$email": customer['customer_email']
                },
                "time": transaction_date,
                "properties": {
                    "Order ID": sale_id,
                    "Categories": list(product_tags),
                    "ItemNames":
                        [line['PRODUCT_TITLE'] for line in sale['lines']],
                    "Skus": [line['sku'] for line in sale['lines']],
                    "RetailStore": sale['retail_store'],
                    "$value": sum(
                        line['price_with_taxes'] for line in sale['lines'])
                }
            }

            jsons_list.append(event_json)
    return jsons_list


def send_to_klaviyo(event_json):
    url = f"{KLAVIYO_API_URL}/track"
    headers = {
        "Authorization": f"Klaviyo-API-Key {KLAVIYO_API_TOKEN}",
        "revision": "2024-06-15",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, json=event_json)
    if response.status_code == 200:
        print("Event successfully sent to Klaviyo")
    else:
        print("Failed to send event to Klaviyo:"
              f"{response.status_code}, {response.text}")


def run_get_retail_sales_to_klaviyo(**context):
    print('[Airflow] Staritng Dag - Retail Sales to Klaviyo ##')
    global customer_updated_count
    print(f'Processing sales from {START_DATE} to {END_DATE}')
    retail_sales_df = get_retail_sales(
        start_date=START_DATE, end_date=END_DATE,
        cust_account=CUSTOMER_ACCOUNT
    )
    print(retail_sales_df.head(20).to_string())

    customers_retail_sales_dict = process_retail_sales_df(retail_sales_df)
    print('## Customer retail sales dictionary ##')
    print(customers_retail_sales_dict)
    total_customer_to_process = len(customers_retail_sales_dict)
    print('## Number of customers to load: ', total_customer_to_process)

    customers_retail_sales_dict = \
        check_skus_in_shopify(customers_retail_sales_dict)
    print('## Retail sales dictionary with Shopify info ##')
    print(customers_retail_sales_dict)

    for customer_email, customer in customers_retail_sales_dict.items():
        print(f'[Airflow] Processing customer to '
              f'send to klaviyo: {customer_email}')
        profile_exists, accepts_marketing = \
            check_klaviyo_profile(customer['customer_email'])
        customer['accepts_marketing'] = accepts_marketing
        if profile_exists:
            klaviyo_existing_customers.add(customer['customer_email'])
        else:
            klaviyo_new_customers.add(customer['customer_email'])
            print(f'[Airflow] Klaviyo profile not found for '
                  f'{customer["customer_email"]}. '
                  'Fetching last 12 months sales.')
            twelve_months_ago = \
                (pd.Timestamp.now() - pd.DateOffset(months=12)
                 ).strftime('%Y-%m-%d')
            recent_sales_df = get_retail_sales(
                start_date=twelve_months_ago,
                end_date=pd.Timestamp.now().strftime('%Y-%m-%d'),
                cust_account=customer["customer_RUT"])
            if recent_sales_df.empty:
                print(f'[Airflow] Error getting 12 months sales for '
                      f'{customer["customer_email"]}. ')
                continue
            recent_customers_retail_sales_dict = \
                process_retail_sales_df(recent_sales_df)
            recent_customers_retail_sales_dict = \
                check_skus_in_shopify(recent_customers_retail_sales_dict)
            customer = \
                recent_customers_retail_sales_dict.get(
                    customer_email, customer)
            customer['accepts_marketing'] = False

        print('## Klaviyo JSON ##')
        klaviyo_json_customer_list = \
            create_klaviyo_json_list(
                customer, show_location_info=False
            )
        for i in klaviyo_json_customer_list:
            print('## Sending to Klaviyo ##')
            print(f'~{i["properties"]["Order ID"]} - {i["event"]} {i["time"]}')
            # print(json.dumps(i, indent=4))
            send_to_klaviyo(i)

        print('## Klaviyo JSON Event 2 ##')
        klaviyo_json_customer_list = \
            create_klaviyo_json_list(
                customer, show_location_info=True
            )
        for i in klaviyo_json_customer_list:
            print('## Sending to Klaviyo ##')
            print(f'~{i["properties"]["Order ID"]} - {i["event"]} {i["time"]}')
            # print(json.dumps(i, indent=4))
            send_to_klaviyo(i)
        customer_updated_count += 1
        print(f'## Customer processes: {customer_updated_count}'
              f'/{total_customer_to_process} ##')

    print("\n### Summary ###")
    print(f"Number of initial sales processed: {len(processed_sales)}")
    print(f"Number of initial customers processed: {len(processed_customers)}")
    print(f"Number of customers with email "
          f"found in processed sales: {len(email_found_in_processed_sale)}")
    print(f"Number of customers with email in PRIMARYCONTACT "
          f"found in ERP: {len(email_found_in_erp_customers_primary_contact)}")
    print(f"Number of customers with email in RECEIPTMAIL"
          f"found in ERP: {len(email_found_in_erp_customers_receipt_mail)}")
    print(f"Number of customers with email "
          f"found in Shopify: {len(email_found_in_shopify)}")
    print(f"Number of customers with no email "
          f"found: {len(email_not_found)}")
    print(f"Number of customers already "
          f"in Klaviyo: {len(klaviyo_existing_customers)}")
    print(f"Number of new customers added "
          f"to Klaviyo: {len(klaviyo_new_customers)}")
    print("---")
    print("\n### LISTS ###")
    print(f"Sales IDs: {list(processed_sales)}")
    print(f"Customer Accounts: {list(processed_customers)}")
    print(f"Emails Found in Processed "
          f"Emails: {list(email_found_in_processed_sale)}")
    print(f"Emails Found in ERP Customers in PRIMARYCONTACT: "
          f"{list(email_found_in_erp_customers_primary_contact)}")
    print(f"Emails Found in ERP Customers in RECEIPTMAIL: "
          f"{list(email_found_in_erp_customers_receipt_mail)}")
    print(f"Emails Found in Shopify Shipping "
          f"Addresses: {list(email_found_in_shopify)}")
    print(f"Emails Not Found: {list(email_not_found)}")
    print(f"Klaviyo Existing Profiles: {list(klaviyo_existing_customers)}")
    print(f"Klaviyo New Profiles: {list(klaviyo_new_customers)}")


task_get_variants = PythonOperator(
    task_id='get_retail_sales_to_klaviyo',
    python_callable=run_get_retail_sales_to_klaviyo,
    dag=dag,
)
