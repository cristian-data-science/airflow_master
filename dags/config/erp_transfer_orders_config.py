from datetime import timedelta, datetime

# Default variables
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 26, 9, 0),
    'email': [
        'enrique.urrutia@patagonia.com',
    ],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'snowflake_conn_id': 'patagonia_snowflake_connection',
    # Número de días hacia atrás para consultar TRs
    'days_lookback': 30,
}

# Columnas para la tabla ERP_TRANSFER_ORDERS
erp_transfer_orders_columns = [
    ('TRANSFER_ORDER_NUMBER', 'STRING'),
    ('TRANSFER_ORDER_STATUS', 'STRING'),
    ('REQUESTED_RECEIPT_DATE', 'TIMESTAMP_NTZ'),
    ('REQUESTED_SHIPPING_DATE', 'TIMESTAMP_NTZ'),
    ('SHIPPING_WAREHOUSE_ID', 'STRING'),
    ('RECEIVING_WAREHOUSE_ID', 'STRING'),
    ('TRANSIT_WAREHOUSE_ID', 'STRING'),
    ('SHIPPING_ADDRESS_LOCATION_ID', 'STRING'),
    ('RECEIVING_ADDRESS_LOCATION_ID', 'STRING'),
    ('SHIPPING_ADDRESS_NAME', 'STRING'),
    ('SHIPPING_ADDRESS_STREET', 'STRING'),
    ('SHIPPING_ADDRESS_CITY', 'STRING'),
    ('SHIPPING_ADDRESS_DISTRICT_NAME', 'STRING'),
    ('SHIPPING_ADDRESS_STATE_ID', 'STRING'),
    ('RECEIVING_ADDRESS_NAME', 'STRING'),
    ('RECEIVING_ADDRESS_STREET', 'STRING'),
    ('RECEIVING_ADDRESS_CITY', 'STRING'),
    ('RECEIVING_ADDRESS_DISTRICT_NAME', 'STRING'),
    ('RECEIVING_ADDRESS_STATE_ID', 'STRING'),
    ('FORMATTED_SHIPPING_ADDRESS', 'STRING'),
    ('FORMATTED_RECEIVING_ADDRESS', 'STRING'),
    ('AXX_DOC_TYPE_CODE', 'STRING'),
    ('AXX_WMS_LOCATION_ID', 'STRING'),
    ('AXX_DRIVER_ID', 'STRING'),
]

# Columnas para la tabla ERP_TRANSFER_ORDERS_LINES
erp_transfer_orders_lines_columns = [
    ('TRANSFER_ORDER_NUMBER', 'STRING'),
    ('LINE_NUMBER', 'NUMBER'),
    ('LINE_STATUS', 'STRING'),
    ('ITEM_NUMBER', 'STRING'),
    ('PRODUCT_CONFIGURATION_ID', 'STRING'),
    ('PRODUCT_COLOR_ID', 'STRING'),
    ('PRODUCT_SIZE_ID', 'STRING'),
    ('PRODUCT_STYLE_ID', 'STRING'),
    ('INVENTORY_UNIT_SYMBOL', 'STRING'),
    ('IS_AUTOMATICALLY_RESERVED', 'BOOLEAN'),
    ('TRANSFER_QUANTITY', 'NUMBER'),
    ('RECEIVED_QUANTITY', 'NUMBER'),
    ('SHIPPED_QUANTITY', 'NUMBER'),
    ('REMAINING_RECEIVED_QUANTITY', 'NUMBER'),
    ('REMAINING_SHIPPED_QUANTITY', 'NUMBER'),
    ('RECEIVING_INVENTORY_LOT_ID', 'STRING'),
    ('SHIPPING_INVENTORY_LOT_ID', 'STRING'),
    ('RECEIVING_TRANSIT_INVENTORY_LOT_ID', 'STRING'),
    ('SHIPPING_TRANSIT_INVENTORY_LOT_ID', 'STRING'),
    ('SHIPPING_WAREHOUSE_ID', 'STRING'),
    ('SHIPPING_WAREHOUSE_LOCATION_ID', 'STRING'),
    ('SHIPPING_SITE_ID', 'STRING'),
    ('REQUESTED_RECEIPT_DATE', 'TIMESTAMP_NTZ'),
    ('REQUESTED_SHIPPING_DATE', 'TIMESTAMP_NTZ'),
    ('SALES_TAX_ITEM_GROUP_CODE_RECEIPT', 'STRING'),
    ('SALES_TAX_ITEM_GROUP_CODE_SHIPMENT', 'STRING'),
    ('ALLOWED_UNDERDELIVERY_PERCENTAGE', 'NUMBER'),
    ('PRICE_TYPE', 'STRING'),
]
