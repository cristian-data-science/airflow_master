from datetime import timedelta, datetime

# Default variables
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 22),
    'email': ['enrique.urrutia@patagonia.com'],
    'phone': 0,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'snowflake_conn_id': 'patagonia_snowflake_connection',
    'snowflake_shopify_customer_table_columns': [
        ('SHOPIFY_ID', 'NUMBER'),
        ('EMAIL', 'VARCHAR(255)'),
        ('ACCEPTS_MARKETING', 'BOOLEAN'),
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
        ('VERIFIED_EMAIL', 'BOOLEAN')
    ]
}
