from datetime import timedelta, datetime


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 12, 5, 0),
    'email': ['enrique.urrutia@patagonia.com'],
    'phone': 0,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=60),

    'snowflake_target_table_name': 'ERP_PRODUCTS_PRICELIST',

    'snowflake_erp_pricelist_table_columns': [
        ('RecordId', 'NUMBER'),
        ('PriceApplicableFromDate', 'TIMESTAMP_NTZ'),
        ('WillSearchContinue', 'VARCHAR(50)'),
        ('SalesPriceQuantity', 'NUMBER'),
        ('QuantityUnitySymbol', 'VARCHAR(10)'),
        ('ProductNumber', 'VARCHAR(100)'),
        ('AttributeBasedPricingId', 'VARCHAR(100)'),
        ('ProductSizeId', 'VARCHAR(50)'),
        ('ItemNumber', 'VARCHAR(100)'),
        ('ProductVersionId', 'VARCHAR(100)'),
        ('PriceCurrencyCode', 'VARCHAR(10)'),
        ('ToQuantity', 'NUMBER'),
        ('FixedPriceCharges', 'NUMBER'),
        ('WillDeliveryDateControlDisregardLeadTime', 'VARCHAR(50)'),
        ('PriceApplicableToDate', 'TIMESTAMP_NTZ'),
        ('PriceWarehouseId', 'VARCHAR(50)'),
        ('SalesLeadTimeDays', 'NUMBER'),
        ('FromQuantity', 'NUMBER'),
        ('CustomerAccountNumber', 'VARCHAR(100)'),
        ('PriceCustomerGroupCode', 'VARCHAR(100)'),
        ('Price', 'NUMBER'),
        ('PriceSiteId', 'VARCHAR(50)'),
        ('IsGenericCurrencySearchEnabled', 'VARCHAR(50)'),
        ('ProductColorId', 'VARCHAR(50)'),
        ('ProductconfigurationId', 'VARCHAR(50)'),
        ('ProductStyleId', 'VARCHAR(50)')
    ]
}
