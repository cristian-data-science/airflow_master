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
        ('RECORDID', 'NUMBER'),
        ('PRICEAPPLICABLEFROMDATE', 'TIMESTAMP_NTZ'),
        ('WILLSEARCHCONTINUE', 'VARCHAR(50)'),
        ('SALESPRICEQUANTITY', 'NUMBER'),
        ('QUANTITYUNITYSYMBOL', 'VARCHAR(10)'),
        ('PRODUCTNUMBER', 'VARCHAR(100)'),
        ('ATTRIBUTEBASEDPRICINGID', 'VARCHAR(100)'),
        ('PRODUCTSIZEID', 'VARCHAR(50)'),
        ('ITEMNUMBER', 'VARCHAR(100)'),
        ('PRODUCTVERSIONID', 'VARCHAR(100)'),
        ('PRICECURRENCYCODE', 'VARCHAR(10)'),
        ('TOQUANTITY', 'NUMBER'),
        ('FIXEDPRICECHARGES', 'NUMBER'),
        ('WILLDELIVERYDATECONTROLDISREGARDLEADTIME', 'VARCHAR(50)'),
        ('PRICEAPPLICABLETODATE', 'TIMESTAMP_NTZ'),
        ('PRICEWAREHOUSEID', 'VARCHAR(50)'),
        ('SALESLEADTIMEDAYS', 'NUMBER'),
        ('FROMQUANTITY', 'NUMBER'),
        ('CUSTOMERACCOUNTNUMBER', 'VARCHAR(100)'),
        ('PRICECUSTOMERGROUPCODE', 'VARCHAR(100)'),
        ('PRICE', 'NUMBER'),
        ('PRICESITEID', 'VARCHAR(50)'),
        ('ISGENERICCURRENCYSEARCHENABLED', 'VARCHAR(50)'),
        ('PRODUCTCOLORID', 'VARCHAR(50)'),
        ('PRODUCTCONFIGURATIONID', 'VARCHAR(50)'),
        ('PRODUCTSTYLEID', 'VARCHAR(50)')
    ]
}
