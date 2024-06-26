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
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
    'snowflake_oms_suborder_line_data_table_columns': [
        ('LINE_ID', 'NUMBER'),
        ('SUBORDER_ID', 'NUMBER'),
        ('ORDER_ID', 'NUMBER'),
        ('AMOUNT_DISCOUNT', 'FLOAT'),
        ('AMOUNT_TAX', 'FLOAT'),
        ('AMOUNT_TOTAL', 'FLOAT'),
        ('AMOUNT_UNTAXED', 'FLOAT'),
        ('STATE_OPTION_NAME', 'VARCHAR(255)'),
        ('DATE_ORDER', 'TIMESTAMP_NTZ'),
        ('DELIVERY_CLIENT_DATE', 'TIMESTAMP_NTZ'),
        ('DELIVERY_METHOD_NAME', 'VARCHAR(255)'),
        ('ECOMMERCE_DATE_ORDER', 'TIMESTAMP_NTZ'),
        ('ECOMMERCE_NAME', 'VARCHAR(255)'),
        ('ECOMMERCE_NAME_CHILD', 'VARCHAR(255)'),
        ('CITY_NAME', 'VARCHAR(255)'),
        ('STREET', 'VARCHAR(255)'),
        ('PHONE', 'VARCHAR(255)'),
        ('STATE_NAME', 'VARCHAR(255)'),
        ('DISCOUNT', 'FLOAT'),
        ('PRICE_REDUCE', 'FLOAT'),
        ('PRICE_TOTAL', 'FLOAT'),
        ('PRICE_UNIT', 'FLOAT'),
        ('PRODUCT_UOM_QTY', 'FLOAT'),
        ('DEFAULT_CODE', 'VARCHAR(255)'),
        ('PRODUCT_NAME', 'VARCHAR(255)'),
        ('EMAIL', 'VARCHAR(255)'),
        ('PARTNER_NAME', 'VARCHAR(255)'),
        ('PARTNER_PHONE', 'VARCHAR(255)'),
        ('PARTNER_STREET', 'VARCHAR(255)'),
        ('PARTNER_VAT', 'VARCHAR(255)'),
        ('PAYMENT_METHOD_NAME', 'VARCHAR(255)'),
        ('WAREHOUSE', 'VARCHAR(255)'),
        ('TRANSFER_WAREHOUSE', 'VARCHAR(255)')
    ],
    'snowflake_oms_suborder_status_history_data_table_columns': [
        ('PRIMARY_KEY', 'STRING'),
        ('ECOMMERCE_NAME_CHILD', 'STRING'),
        ('STATUS', 'STRING'),
        ('REGISTER_DATE', 'TIMESTAMP_NTZ')
    ]

}
