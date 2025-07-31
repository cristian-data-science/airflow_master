from datetime import timedelta, datetime

# Default variables
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 30, 9, 0),
    'email': ['enrique.urrutia@patagonia.com'],
    'phone': 0,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'snowflake_conn_id': 'patagonia_snowflake_connection',
    'erp_sales_invoice_table_columns': [
        ('DATAAREAID', 'STRING'),
        ('INVOICENUMBER', 'STRING'),
        ('INVOICEDATE', 'TIMESTAMP_NTZ'),
        ('LEDGERVOUCHER', 'STRING'),
        ('INVOICEADDRESSCOUNTRYREGIONISOCODE', 'STRING'),
        ('INVOICEADDRESSZIPCODE', 'STRING'),
        ('TOTALTAXAMOUNT', 'FLOAT'),
        ('INVOICEADDRESSSTREETNUMBER', 'STRING'),
        ('TOTALDISCOUNTCUSTOMERGROUPCODE', 'FLOAT'),
        ('INVOICEADDRESSSTREET', 'STRING'),
        ('TOTALCHARGEAMOUNT', 'FLOAT'),
        ('TOTALDISCOUNTAMOUNT', 'FLOAT'),
        ('CURRENCYCODE', 'STRING'),
        ('SALESORDERNUMBER', 'STRING'),
        ('DELIVERYTERMSCODE', 'STRING'),
        ('CONTACTPERSONID', 'STRING'),
        ('SALESORDERRESPONSIBLEPERSONNELNUMBER', 'STRING'),
        ('PAYMENTTERMSNAME', 'STRING'),
        ('DELIVERYMODECODE', 'STRING'),
        ('INVOICECUSTOMERACCOUNTNUMBER', 'STRING'),
        ('INVOICEADDRESSCITY', 'STRING'),
        ('INVOICEADDRESSSTATE', 'STRING'),
        ('INVOICEADDRESSCOUNTRYREGIONID', 'STRING'),
        ('CUSTOMERSORDERREFERENCE', 'STRING'),
        ('TOTALINVOICEAMOUNT', 'FLOAT'),
        ('SALESORDERORIGINCODE', 'STRING')
    ],
    # Configuración para días hacia atrás por defecto
    # (puede ser sobreescrito por Variables de Airflow)
    'default_days_back': 30,
    # Tamaño de página para la paginación de OData
    'page_size': 10000,
    # Timeout para requests HTTP
    'request_timeout': 120
}
