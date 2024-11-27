from datetime import timedelta, datetime

# Default variables
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 21, 5, 0),
    'email': ['enrique.urrutia@patagonia.com'],
    'phone': 0,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
    'byod_erp_products_table_name': 'Products',
    # Snowflake table columns
    'snowflake_erp_products_table_columns': [
        ('SKU', 'VARCHAR(255)'),
        ('ESTILOCOLOR', 'VARCHAR(255)'),
        ('ITEMNUMBER', 'VARCHAR(255)'),
        ('CONFIGURATION', 'VARCHAR(255)'),
        ('SIZE', 'VARCHAR(255)'),
        ('COLOR', 'VARCHAR(255)'),
        ('COLORNAME', 'VARCHAR(255)'),
        ('GENERICCOLOR', 'VARCHAR(255)'),
        ('TEMPCL', 'VARCHAR(255)'),
        ('CATEGORIA', 'VARCHAR(255)'),
        ('DEPARTMENT', 'VARCHAR(255)'),
        ('BUSINESSDIVISION', 'VARCHAR(255)'),
        ('BUSINESSUNIT', 'VARCHAR(255)'),
        ('TEAM', 'VARCHAR(255)'),
        ('CATEGORY', 'VARCHAR(255)'),
        ('CLASS', 'VARCHAR(255)'),
        ('TEAMCATEGORY', 'VARCHAR(255)'),
        ('PRODUCTNAME', 'VARCHAR(255)'),
        ('TIPOPRODUCTO', 'VARCHAR(255)'),
        ('GENDER', 'VARCHAR(255)'),
        ('FAMILIA', 'VARCHAR(255)'),
        ('COSTO_HISTORICO', 'FLOAT'),
        ('SEASON_USA', 'VARCHAR(255)')
    ]
}
