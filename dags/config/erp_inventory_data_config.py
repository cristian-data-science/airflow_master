from datetime import timedelta, datetime

# Default variables
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 23, 9, 0),
    'email': ['enrique.urrutia@patagonia.com'],
    'phone': 0,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
    'snowflake_conn_id': 'patagonia_snowflake_connection',
    'byod_erp_inventory_table_name':
        'InventWarehouseInventoryStatusOnHandV2Staging',
    'byod_erp_inventory_table_interest_columns': [
        ('ITEMNUMBER', 'STRING'),
        ('PRODUCTNAME', 'STRING'),
        ('PRODUCTCOLORID', 'STRING'),
        ('PRODUCTCONFIGURATIONID', 'STRING'),
        ('PRODUCTSIZEID', 'STRING'),
        ('PRODUCTSTYLEID', 'STRING'),
        ('INVENTORYSITEID', 'STRING'),
        ('INVENTORYWAREHOUSEID', 'STRING'),
        ('INVENTORYSTATUSID', 'STRING'),
        ('ONHANDQUANTITY', 'FLOAT'),
        ('RESERVEDONHANDQUANTITY', 'FLOAT'),
        ('AVAILABLEONHANDQUANTITY', 'FLOAT'),
        ('ORDEREDQUANTITY', 'FLOAT'),
        ('RESERVEDORDEREDQUANTITY', 'FLOAT'),
        ('AVAILABLEORDEREDQUANTITY', 'FLOAT'),
        ('ONORDERQUANTITY', 'FLOAT'),
        ('TOTALAVAILABLEQUANTITY', 'FLOAT'),
        ('SYNCSTARTDATETIME', 'TIMESTAMP_NTZ')
    ],
    'byod_erp_inventory_table_columns': [
        ('ITEMNUMBER', 'STRING'),
        ('PRODUCTNAME', 'STRING'),
        ('PRODUCTCOLORID', 'STRING'),
        ('PRODUCTCONFIGURATIONID', 'STRING'),
        ('PRODUCTSIZEID', 'STRING'),
        ('SKU', 'STRING'),
        ('PRODUCTSTYLEID', 'STRING'),
        ('INVENTORYSITEID', 'STRING'),
        ('INVENTORYWAREHOUSEID', 'STRING'),
        ('INVENTORYSTATUSID', 'STRING'),
        ('ONHANDQUANTITY', 'FLOAT'),
        ('RESERVEDONHANDQUANTITY', 'FLOAT'),
        ('AVAILABLEONHANDQUANTITY', 'FLOAT'),
        ('ORDEREDQUANTITY', 'FLOAT'),
        ('RESERVEDORDEREDQUANTITY', 'FLOAT'),
        ('AVAILABLEORDEREDQUANTITY', 'FLOAT'),
        ('ONORDERQUANTITY', 'FLOAT'),
        ('TOTALAVAILABLEQUANTITY', 'FLOAT'),
        ('SYNCSTARTDATETIME', 'TIMESTAMP_NTZ')
    ],
    'wms_inventory_table_columns': [
        ('IDITEM', 'NUMBER'),
        ('ITEMCODE', 'STRING'),
        ('SHORTITEMNAME', 'STRING'),
        ('ITEMDESCRIPTION', 'STRING'),
        ('QTYSTOCK', 'FLOAT'),
        ('QTYCICLECOUNT', 'FLOAT'),
        ('QTYRESERVED', 'FLOAT'),
        ('QTYRECEIVED', 'FLOAT'),
        ('QTYSTG', 'FLOAT'),
        ('QTYSTGD', 'FLOAT'),
        ('QTYSTGR', 'FLOAT'),
        ('QTYPENDINGPICKING', 'FLOAT'),
        ('QTYTASKPICKING', 'FLOAT'),
        ('QTYTASKSIMULATION', 'FLOAT'),
        ('QTYHOLDED', 'FLOAT'),
        ('QTYDOCK', 'FLOAT'),
        ('QTYTRUCK', 'FLOAT'),
        ('QTYTOTAL', 'FLOAT'),
        ('QTYUTILIZADA', 'FLOAT'),
        ('FECHAREGISTRO', 'TIMESTAMP')
    ],
}
