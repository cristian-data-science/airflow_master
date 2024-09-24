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
    'snowflake_shopify_orders_line_table_columns': [
        ('ORDER_ID', 'VARCHAR(255)' )  
        ('LINE_ITEM_ID','VARCHAR(255)' )
        ('ORDER_NAME','VARCHAR(255)' )
        ('SKU', 'VARCHAR(255)' )              
        ('QUANTITY', 'VARCHAR(255)' )   
    ]
}
