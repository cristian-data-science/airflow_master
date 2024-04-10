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
    'snowflake_oms_order_incidence_table_columns': [
        ('ECOMMERCE_NAME_CHILD', 'VARCHAR(255)'),
        ('INCIDENCE_CREATE_DATE', 'TIMESTAMP_NTZ'),
        ('LAST_REGISTER_DATE', 'TIMESTAMP_NTZ'),
        ('DESCRIPTION', 'VARCHAR(1024)'),
        ('NAME', 'VARCHAR(255)'),
        ('STATE', 'VARCHAR(255)'),
        ('USER', 'VARCHAR(255)')
    ],
    'snowflake_oms_history_incidence_table_columns': [
        ('PRIMARY_KEY', 'VARCHAR(255)'),
        ('ECOMMERCE_NAME_CHILD', 'VARCHAR(255)'),
        ('CREATE_DATE', 'TIMESTAMP_NTZ'),
        ('DESCRIPTION', 'VARCHAR(1024)'),
        ('NAME', 'VARCHAR(255)'),
        ('STATE', 'VARCHAR(255)'),
        ('USER', 'VARCHAR(255)')
    ]

}
