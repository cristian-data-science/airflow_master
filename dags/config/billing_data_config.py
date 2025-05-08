from datetime import timedelta, datetime

# Default variables
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 8, 9, 0),  # Starting from today
    'email': ['enrique.urrutia@patagonia.com'],
    'phone': 0,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
    'snowflake_conn_id': 'patagonia_snowflake_connection',
    'billing_table_columns': [
        ('ID', 'STRING'),
        ('TIPO_DTE', 'STRING'),
        ('FOLIO', 'STRING'),
        ('FECHA_EMISION', 'TIMESTAMP'),
        ('RUT_RECEPTOR', 'STRING'),
        ('RAZON_SOCIAL_RECEPTOR', 'STRING'),
        ('NETO', 'FLOAT'),
        ('EXENTO', 'FLOAT'),
        ('IVA', 'FLOAT'),
        ('TOTAL', 'FLOAT'),
        ('ESTADO_SII', 'STRING'),
        ('URL', 'STRING')
    ]
}
