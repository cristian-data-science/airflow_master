from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from config.erp_transfer_orders_config import (
    default_args,
    erp_transfer_orders_columns, erp_transfer_orders_lines_columns
)
from utils.utils import write_data_to_snowflake
import os
import requests
import pandas as pd
import logging

load_dotenv()

DAYS = int(Variable.get("erp_transfer_orders_days", default_var="30"))

# Environment variables
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')
ERP_URL = os.getenv('ERP_URL')
ERP_TOKEN_URL = os.getenv('ERP_TOKEN_URL')
ERP_CLIENT_ID = os.getenv('ERP_CLIENT_ID')
ERP_CLIENT_SECRET = os.getenv('ERP_CLIENT_SECRET')


def get_erp_token():
    """
    Obtains an access token from ERP D365 using OAuth2 client credentials.
    """
    if not all([ERP_URL, ERP_TOKEN_URL, ERP_CLIENT_ID, ERP_CLIENT_SECRET]):
        raise ValueError(
            "Missing required ERP environment variables. "
            "Please check ERP_URL, ERP_TOKEN_URL, ERP_CLIENT_ID, "
            "ERP_CLIENT_SECRET"
        )

    token_url = f'{ERP_TOKEN_URL}/oauth2/v2.0/token'
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': ERP_CLIENT_ID,
        'client_secret': ERP_CLIENT_SECRET,
        'scope': f'{ERP_URL}/.default'
    }
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    logging.info(f"[ERP Token] Requesting token from: {token_url}")
    response = requests.post(token_url, data=token_data, headers=headers)
    logging.info(f"[ERP Token] Response status: {response.status_code}")

    if response.status_code != 200:
        logging.error(f"[ERP Token] Error response: {response.text}")
        raise Exception(
            f"Error obtaining ERP token: {response.status_code} "
            f"- {response.text}"
        )

    logging.info("[ERP Token] Token obtained successfully")
    return response.json().get('access_token')


def fetch_transfer_order_headers(token, days_lookback):
    """
    Fetches Transfer Order Headers from ERP D365 API.
    Filters by RequestedReceiptDate within the lookback period.
    """
    logging.info(f"Fetching TR headers for last {days_lookback} days")

    # Calculate date range
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days_lookback)

    start_date_str = start_date.strftime("%Y-%m-%dT00:00:00Z")
    end_date_str = end_date.strftime("%Y-%m-%dT23:59:59Z")

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    all_records = []
    # Use $filter for date range
    filter_query = (
        f"RequestedReceiptDate ge {start_date_str} and "
        f"RequestedReceiptDate le {end_date_str}"
    )
    url = (
        f"{ERP_URL}/data/TransferOrderHeaders"
        f"?$filter={filter_query}"
    )

    page_count = 0
    while url:
        page_count += 1
        logging.info(f"[ERP Headers] Page {page_count} - Request URL: {url}")
        response = requests.get(url, headers=headers)
        logging.info(
            f"[ERP Headers] Page {page_count} - "
            f"Response status: {response.status_code}"
        )

        if response.status_code != 200:
            logging.error(
                f"[ERP Headers] Error response: {response.text[:500]}"
            )
            raise Exception(
                f"Error fetching TR headers: {response.status_code} "
                f"- {response.text[:500]}"
            )

        data = response.json()
        records = data.get('value', [])
        all_records.extend(records)
        logging.info(
            f"[ERP Headers] Page {page_count} - "
            f"Fetched {len(records)} records, total so far: {len(all_records)}"
        )

        # Check for pagination
        url = data.get('@odata.nextLink')
        if url:
            logging.info("[ERP Headers] Next page available")

    logging.info(
        f"[ERP Headers] Completed - Total pages: {page_count}, "
        f"Total records: {len(all_records)}"
    )
    return all_records


def fetch_transfer_order_lines(token, tr_number):
    """
    Fetches Transfer Order Lines for a specific TR number from ERP D365 API.
    """
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    all_lines = []
    filter_query = f"TransferOrderNumber eq '{tr_number}'"
    url = f"{ERP_URL}/data/TransferOrderLines?$filter={filter_query}"

    page_count = 0
    while url:
        page_count += 1
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            logging.error(
                f"[ERP Lines] TR {tr_number} - ERROR - "
                f"Status: {response.status_code} - {response.text[:200]}"
            )
            return []

        data = response.json()
        lines = data.get('value', [])
        all_lines.extend(lines)
        logging.debug(
            f"[ERP Lines] TR {tr_number} - Page {page_count} - "
            f"Fetched {len(lines)} lines"
        )

        # Check for pagination
        url = data.get('@odata.nextLink')

    if all_lines:
        logging.info(
            f"[ERP Lines] TR {tr_number} - OK - "
            f"Total lines: {len(all_lines)}"
        )
    else:
        logging.warning(
            f"[ERP Lines] TR {tr_number} - No lines found"
        )

    return all_lines


def transform_headers_to_dataframe(headers_data):
    """
    Transforms raw API response to DataFrame matching Snowflake schema.
    """
    if not headers_data:
        return pd.DataFrame()

    records = []
    for h in headers_data:
        records.append({
            'TRANSFER_ORDER_NUMBER': h.get('TransferOrderNumber'),
            'TRANSFER_ORDER_STATUS': h.get('TransferOrderStatus'),
            'REQUESTED_RECEIPT_DATE': h.get('RequestedReceiptDate'),
            'REQUESTED_SHIPPING_DATE': h.get('RequestedShippingDate'),
            'SHIPPING_WAREHOUSE_ID': h.get('ShippingWarehouseId'),
            'RECEIVING_WAREHOUSE_ID': h.get('ReceivingWarehouseId'),
            'TRANSIT_WAREHOUSE_ID': h.get('TransitWarehouseId'),
            'SHIPPING_ADDRESS_LOCATION_ID': h.get('ShippingAddressLocationId'),
            'RECEIVING_ADDRESS_LOCATION_ID': h.get(
                'ReceivingAddressLocationId'),
            'SHIPPING_ADDRESS_NAME': h.get('ShippingAddressName'),
            'SHIPPING_ADDRESS_STREET': h.get('ShippingAddressStreet'),
            'SHIPPING_ADDRESS_CITY': h.get('ShippingAddressCity'),
            'SHIPPING_ADDRESS_DISTRICT_NAME': h.get(
                'ShippingAddressDistrictName'),
            'SHIPPING_ADDRESS_STATE_ID': h.get('ShippingAddressStateId'),
            'RECEIVING_ADDRESS_NAME': h.get('ReceivingAddressName'),
            'RECEIVING_ADDRESS_STREET': h.get('ReceivingAddressStreet'),
            'RECEIVING_ADDRESS_CITY': h.get('ReceivingAddressCity'),
            'RECEIVING_ADDRESS_DISTRICT_NAME': h.get(
                'ReceivingAddressDistrictName'),
            'RECEIVING_ADDRESS_STATE_ID': h.get('ReceivingAddressStateId'),
            'FORMATTED_SHIPPING_ADDRESS': h.get('FormattedShippingAddress'),
            'FORMATTED_RECEIVING_ADDRESS': h.get('FormattedReceivingAddress'),
            'AXX_DOC_TYPE_CODE': h.get('AXXDocTypeCode'),
            'AXX_WMS_LOCATION_ID': h.get('AXXWMSLocationId'),
            'AXX_DRIVER_ID': h.get('AXXDriverID'),
        })

    df = pd.DataFrame(records)

    # Convert date columns
    date_columns = ['REQUESTED_RECEIPT_DATE', 'REQUESTED_SHIPPING_DATE']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

    return df


def transform_lines_to_dataframe(lines_data):
    """
    Transforms raw API response to DataFrame matching Snowflake schema.
    """
    if not lines_data:
        return pd.DataFrame()

    records = []
    for line in lines_data:
        # Convert IsAutomaticallyReserved from "Yes"/"No" to boolean
        is_reserved = line.get('IsAutomaticallyReserved', 'No')
        is_reserved_bool = True if is_reserved == 'Yes' else False

        records.append({
            'TRANSFER_ORDER_NUMBER': line.get('TransferOrderNumber'),
            'LINE_NUMBER': line.get('LineNumber'),
            'LINE_STATUS': line.get('LineStatus'),
            'ITEM_NUMBER': line.get('ItemNumber'),
            'PRODUCT_CONFIGURATION_ID': line.get('ProductConfigurationId'),
            'PRODUCT_COLOR_ID': line.get('ProductColorId'),
            'PRODUCT_SIZE_ID': line.get('ProductSizeId'),
            'PRODUCT_STYLE_ID': line.get('ProductStyleId'),
            'INVENTORY_UNIT_SYMBOL': line.get('InventoryUnitSymbol'),
            'IS_AUTOMATICALLY_RESERVED': is_reserved_bool,
            'TRANSFER_QUANTITY': line.get('TransferQuantity'),
            'RECEIVED_QUANTITY': line.get('ReceivedQuantity'),
            'SHIPPED_QUANTITY': line.get('ShippedQuantity'),
            'REMAINING_RECEIVED_QUANTITY': line.get(
                'RemainingReceivedQuantity'),
            'REMAINING_SHIPPED_QUANTITY': line.get('RemainingShippedQuantity'),
            'RECEIVING_INVENTORY_LOT_ID': line.get('ReceivingInventoryLotId'),
            'SHIPPING_INVENTORY_LOT_ID': line.get('ShippingInventoryLotId'),
            'RECEIVING_TRANSIT_INVENTORY_LOT_ID': line.get(
                'ReceivingTransitInventoryLotId'),
            'SHIPPING_TRANSIT_INVENTORY_LOT_ID': line.get(
                'ShippingTransitInventoryLotId'),
            'SHIPPING_WAREHOUSE_ID': line.get('ShippingWarehouseId'),
            'SHIPPING_WAREHOUSE_LOCATION_ID': line.get(
                'ShippingWarehouseLocationId'),
            'SHIPPING_SITE_ID': line.get('ShippingSiteId'),
            'REQUESTED_RECEIPT_DATE': line.get('RequestedReceiptDate'),
            'REQUESTED_SHIPPING_DATE': line.get('RequestedShippingDate'),
            'SALES_TAX_ITEM_GROUP_CODE_RECEIPT': line.get(
                'SalesTaxItemGroupCodeReceipt'),
            'SALES_TAX_ITEM_GROUP_CODE_SHIPMENT': line.get(
                'SalesTaxItemGroupCodeShipment'),
            'ALLOWED_UNDERDELIVERY_PERCENTAGE': line.get(
                'AllowedUnderdeliveryPercentage'),
            'PRICE_TYPE': line.get('PriceType'),
        })

    df = pd.DataFrame(records)

    # Convert date columns
    date_columns = ['REQUESTED_RECEIPT_DATE', 'REQUESTED_SHIPPING_DATE']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

    return df


def run_get_transfer_orders(**context):
    """
    Main function to fetch Transfer Order Headers and load into Snowflake.
    """
    logging.info("[Start] Fetching Transfer Order Headers from ERP")

    # Get token
    token = get_erp_token()
    logging.info("Successfully obtained ERP token")

    # Fetch headers - DAYS from Airflow Variable takes priority
    days_lookback = DAYS
    logging.info(f"Using days_lookback: {days_lookback}")
    headers_data = fetch_transfer_order_headers(token, days_lookback)

    if not headers_data:
        logging.warning("No Transfer Order Headers found")
        return {'headers_count': 0}

    # Transform to DataFrame
    df_headers = transform_headers_to_dataframe(headers_data)
    logging.info(f"Transformed {len(df_headers)} headers to DataFrame")
    logging.info(f"Sample data:\n{df_headers.head()}")

    # Write to Snowflake
    if not df_headers.empty:
        write_data_to_snowflake(
            df_headers,
            'ERP_TRANSFER_ORDERS',
            erp_transfer_orders_columns,
            ['TRANSFER_ORDER_NUMBER'],
            'TEMP_ERP_TRANSFER_ORDERS',
            SNOWFLAKE_CONN_ID
        )
        logging.info(
            f"Successfully wrote {len(df_headers)} TR headers to Snowflake")

    # Store TR numbers for the lines task
    tr_numbers = df_headers['TRANSFER_ORDER_NUMBER'].tolist()
    context['ti'].xcom_push(key='tr_numbers', value=tr_numbers)

    return {'headers_count': len(df_headers)}


def run_get_transfer_order_lines(**context):
    """
    Main function to fetch Transfer Order Lines for all TRs
    and load into Snowflake.
    """
    logging.info("[Start] Fetching Transfer Order Lines from ERP")

    # Get TR numbers from previous task
    tr_numbers = context['ti'].xcom_pull(
        key='tr_numbers', task_ids='get_transfer_orders')

    if not tr_numbers:
        logging.warning("No TR numbers found, skipping lines fetch")
        return {'lines_count': 0}

    logging.info(f"Will fetch lines for {len(tr_numbers)} TRs")

    # Get token
    token = get_erp_token()

    # Fetch lines for each TR
    all_lines = []
    for i, tr_number in enumerate(tr_numbers):
        if i % 50 == 0:
            logging.info(f"Progress: {i}/{len(tr_numbers)} TRs processed")

        lines = fetch_transfer_order_lines(token, tr_number)
        all_lines.extend(lines)

        # Refresh token every 100 requests to avoid expiration
        if i > 0 and i % 100 == 0:
            token = get_erp_token()
            logging.info("Refreshed ERP token")

    logging.info(f"Total lines fetched: {len(all_lines)}")

    if not all_lines:
        logging.warning("No Transfer Order Lines found")
        return {'lines_count': 0}

    # Transform to DataFrame
    df_lines = transform_lines_to_dataframe(all_lines)
    logging.info(f"Transformed {len(df_lines)} lines to DataFrame")
    logging.info(f"Sample data:\n{df_lines.head()}")

    # Write to Snowflake
    if not df_lines.empty:
        write_data_to_snowflake(
            df_lines,
            'ERP_TRANSFER_ORDERS_LINES',
            erp_transfer_orders_lines_columns,
            ['TRANSFER_ORDER_NUMBER', 'LINE_NUMBER'],
            'TEMP_ERP_TRANSFER_ORDERS_LINES',
            SNOWFLAKE_CONN_ID
        )
        logging.info(
            f"Successfully wrote {len(df_lines)} TR lines to Snowflake")

    return {'lines_count': len(df_lines)}


# DAG definition
dag = DAG(
    'erp_transfer_orders_data',
    default_args=default_args,
    description='DAG to extract Transfer Orders and Lines from ERP D365 '
                'and load them into Snowflake',
    schedule_interval='0 8,18 * * *',
    catchup=False,
    tags=['erp', 'transfer_orders']
)

task_get_headers = PythonOperator(
    task_id='get_transfer_orders',
    python_callable=run_get_transfer_orders,
    dag=dag,
)

task_get_lines = PythonOperator(
    task_id='get_transfer_order_lines',
    python_callable=run_get_transfer_order_lines,
    dag=dag,
)

# Task dependencies
task_get_headers >> task_get_lines
