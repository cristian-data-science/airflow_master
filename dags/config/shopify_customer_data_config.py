from datetime import timedelta, datetime

# Default variables
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 10, 5, 0),
    'email': ['enrique.urrutia@patagonia.com'],
    'phone': 0,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'snowflake_shopify_customer_table_columns': [
        ('SHOPIFY_ID', 'NUMBER'),
        ('EMAIL', 'VARCHAR(255)'),
        ('ACCEPTS_MARKETING', 'VARCHAR(50)'),
        ('ACCEPTS_MARKETING_UPDATED_AT', 'TIMESTAMP_NTZ'),
        ('ADMIN_GRAPHQL_API_ID', 'VARCHAR(255)'),
        ('CREATED_AT', 'TIMESTAMP_NTZ'),
        ('CURRENCY', 'VARCHAR(255)'),
        ('FIRST_NAME', 'VARCHAR(255)'),
        ('LAST_NAME', 'VARCHAR(255)'),
        ('LAST_ORDER_ID', 'NUMBER'),
        ('LAST_ORDER_NAME', 'VARCHAR(255)'),
        ('MARKETING_OPT_IN_LEVEL', 'VARCHAR(255)'),
        ('MULTIPASS_IDENTIFIER', 'VARCHAR(255)'),
        ('NOTE', 'VARCHAR(255)'),
        ('ORDERS_COUNT', 'NUMBER'),
        ('PHONE', 'VARCHAR(255)'),
        ('STATE', 'VARCHAR(255)'),
        ('TAGS', 'VARCHAR(255)'),
        ('TAX_EXEMPT', 'BOOLEAN'),
        ('TOTAL_SPENT', 'FLOAT'),
        ('UPDATED_AT', 'TIMESTAMP_NTZ'),
        ('VERIFIED_EMAIL', 'BOOLEAN'),
        ('MOST_REPEATED_NAME', 'VARCHAR(255)'),
        ('MOST_REPEATED_PHONE', 'VARCHAR(255)'),
        ('MOST_REPEATED_ADDRESS1', 'VARCHAR(255)'),
        ('MOST_REPEATED_ADDRESS2', 'VARCHAR(255)'),
        ('MOST_REPEATED_RUT', 'VARCHAR(255)'),
        ('MOST_REPEATED_PROVINCE', 'VARCHAR(255)'),
        ('MOST_REPEATED_CITY', 'VARCHAR(255)')
    ],
    'snowflake_shopify_customer_addresses_table_columns': [
        ('SHOPIFY_ID', 'BIGINT'),
        ('CUSTOMER_ID', 'BIGINT'),
        ('FIRST_NAME', 'VARCHAR(255)'),
        ('LAST_NAME', 'VARCHAR(255)'),
        ('COMPANY', 'VARCHAR(255)'),
        ('ADDRESS1', 'VARCHAR(255)'),
        ('ADDRESS2', 'VARCHAR(255)'),
        ('CITY', 'VARCHAR(255)'),
        ('PROVINCE', 'VARCHAR(255)'),
        ('COUNTRY', 'VARCHAR(255)'),
        ('ZIP', 'VARCHAR(255)'),
        ('PHONE', 'VARCHAR(255)'),
        ('NAME', 'VARCHAR(255)'),
        ('PROVINCE_CODE', 'VARCHAR(255)'),
        ('COUNTRY_CODE', 'VARCHAR(255)'),
        ('COUNTRY_NAME', 'VARCHAR(255)'),
        ('DEFAULT', 'BOOLEAN')
    ]
}
