from datetime import timedelta, datetime

# Default variables
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 15, 5, 0),
    'email': [
        'enrique.urrutia@patagonia.com',
        'sebastian.bahamondes@patagonia.com',
        'juan.valdes@patagonia.com'
    ],
    'phone': 0,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
    'byod_erp_accounting_table_name': 'GAPdeAccountingTransactionStaging',
    # Snowflake table columns
    'snowflake_erp_accounting_table_columns': [
        ('DEFINITIONGROUP', 'VARCHAR(255)'),
        ('EXECUTIONID', 'VARCHAR(255)'),
        ('ISSELECTED', 'BOOLEAN'),
        ('TRANSFERSTATUS', 'BOOLEAN'),
        ('GENERALJOURNALACCOUNTENTRY_RECID', 'NUMBER'),
        ('JOURNALNUMBER', 'VARCHAR(255)'),
        ('SUBLEDGERVOUCHER', 'VARCHAR(255)'),
        ('ACCOUNTINGDATE', 'DATE'),
        ('DOCUMENTDATE', 'DATE'),
        ('DOCUMENTNUMBER', 'VARCHAR(255)'),
        ('GAPCREATEDBY', 'VARCHAR(255)'),
        ('SUBLEDGERVOUCHERDATAAREAID', 'VARCHAR(50)'),
        ('LEDGERACCOUNT', 'VARCHAR(50)'),
        ('MAINACCOUNTNAME', 'VARCHAR(255)'),
        ('TRANSACTIONCURRENCYCODE', 'VARCHAR(10)'),
        ('TRANSACTIONCURRENCYAMOUNT', 'NUMBER(38,6)'),
        ('ACCOUNTINGCURRENCYAMOUNT', 'NUMBER(38,6)'),
        ('SALESID', 'VARCHAR(255)'),
        ('PURCHID', 'VARCHAR(255)'),
        ('DESCRIPTION', 'VARCHAR(500)'),
        ('AXXDOCTYPECODE', 'VARCHAR(255)'),
        ('AXXDESCRIPTIONERROR', 'VARCHAR(500)'),
        ('AXXTAXDOCTYPE', 'VARCHAR(255)'),
        ('CUSTOMERNAME', 'VARCHAR(255)'),
        ('INVOICEID', 'VARCHAR(255)'),
        ('INVOICEACCOUNT', 'VARCHAR(50)'),
        ('GAPFINANCIALDIMENSIONS', 'VARCHAR(500)'),
        ('GAPCECODIMENSION', 'VARCHAR(50)'),
        ('GAPCANALDIMENSION', 'VARCHAR(50)'),
        ('GAPPOSTINGTYPE', 'VARCHAR(255)'),
        ('GAPPOSTINGLAYER', 'VARCHAR(50)'),
        ('PARTITION', 'VARCHAR(50)'),
        ('DATAAREAID', 'VARCHAR(50)'),
        ('SYNCSTARTDATETIME', 'TIMESTAMP')
    ]
}
