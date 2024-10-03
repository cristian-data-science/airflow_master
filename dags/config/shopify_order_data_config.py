from datetime import timedelta, datetime

# Default variables
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 10, 5, 0),
    'email': ['enrique.urrutia@patagonia.com'],
    'phone': 0,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
    'snowflake_shopify_order_table_columns': [
        ('ORDER_ID', 'NUMBER'),  # id
        ('EMAIL', 'VARCHAR(255)'),  # contact_email
        ('CREATED_AT', 'TIMESTAMP_NTZ'),  # created_at
        ('CURRENT_SUBTOTAL_PRICE', 'FLOAT'),  # current_subtotal_price
        ('CURRENT_TOTAL_DISCOUNTS', 'FLOAT'),  # current_total_discounts
        ('CURRENT_TOTAL_PRICE', 'FLOAT'),  # current_total_price
        ('FINANCIAL_STATUS', 'VARCHAR(255)'),  # financial_status
        ('NAME', 'VARCHAR(255)'),  # name
        ('PROCESSED_AT', 'TIMESTAMP_NTZ'),  # processed_at
        ('SUBTOTAL_PRICE', 'FLOAT'),  # subtotal_price
        ('UPDATED_AT', 'TIMESTAMP_NTZ'),  # updated_at
        ('CUSTOMER_ID', 'NUMBER'),  # customer.id
        ('SMS_MARKETING_CONSENT', 'VARCHAR(255)'),
        # customer.sms_marketing_consent
        ('TAGS', 'VARCHAR(255)'),  # customer.tags
        ('ACCEPTS_MARKETING', 'BOOLEAN'),
        # customer.accepts_marketing
        ('ACCEPTS_MARKETING_UPDATED_AT', 'TIMESTAMP_NTZ'),
        # customer.accepts_marketing_updated_at
        ('MARKETING_OPT_IN_LEVEL', 'VARCHAR(255)'),
        # customer.marketing_opt_in_level
        ('DISCOUNTED_PRICE', 'FLOAT'),
        # shipping_lines.discounted_price
    ],
    'snowflake_shopify_shipping_address_table_columns': [
        ('ORDER_ID', 'VARCHAR(255)'),
        ('CUSTOMER_ID', 'VARCHAR(255)'),
        ('EMAIL', 'VARCHAR(255)'),
        ('ORDER_DATE', 'TIMESTAMP_NTZ'),
        ('FIRST_NAME', 'VARCHAR(255)'),
        ('LAST_NAME', 'VARCHAR(255)'),
        ('ADDRESS1', 'VARCHAR(255)'),
        ('ADDRESS2', 'VARCHAR(255)'),
        ('CITY', 'VARCHAR(255)'),
        ('ZIP', 'VARCHAR(255)'),
        ('PROVINCE', 'VARCHAR(255)'),
        ('COUNTRY', 'VARCHAR(255)'),
        ('PHONE', 'VARCHAR(255)'),
        ('LATITUDE', 'FLOAT'),
        ('LONGITUDE', 'FLOAT'),
        ('ACCEPTS_MARKETING', 'BOOLEAN'),
        ('MARKETING_OPT_IN_LEVEL', 'VARCHAR(255)')
    ],
    'snowflake_shopify_orders_line_table_columns': [
        ('ORDER_ID', 'INT' ), 
        ('LINE_ITEM_ID','VARCHAR(255)' ),
        ('ORDER_NAME','VARCHAR(255)' ),
        ('SKU', 'VARCHAR(255)' ),             
        ('QUANTITY', 'VARCHAR(255)' )   
    ]
}

