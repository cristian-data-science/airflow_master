from datetime import timedelta, datetime

# Default variables
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 4, 5, 0),
    'email': ['enrique.urrutia@patagonia.com'],
    'phone': 0,
    'email_on_failure': True,
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
    ]
}
