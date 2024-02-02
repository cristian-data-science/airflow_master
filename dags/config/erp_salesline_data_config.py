from datetime import timedelta, datetime

# Default variables
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 3, 5, 0),
    'email': ['enrique.urrutia@patagonia.com'],
    'phone': 0,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
    'byod_erp_sales_table_name': 'GAPdSalesLineCostStaging',
    # Not include snowflake date's column
    'snowflake_erp_salesline_table_columns': [
        ('SALESLINEPK', 'VARCHAR(500)'),
        ('DEFINITIONGROUP', 'VARCHAR(255)'),
        ('EXECUTIONID', 'VARCHAR(255)'),
        ('ISSELECTED', 'INT'),
        ('TRANSFERSTATUS', 'INT'),
        ('ADDRESSREFTABLEID', 'INT'),
        ('BARCODE', 'VARCHAR(255)'),
        ('BARCODETYPE', 'VARCHAR(50)'),
        ('CONFIRMEDDLV', 'DATE'),
        ('COSTPRICE', 'FLOAT'),
        ('CURRENCYCODE', 'VARCHAR(50)'),
        ('CUSTACCOUNT', 'VARCHAR(255)'),
        ('CUSTGROUP', 'VARCHAR(50)'),
        ('CUSTOMERREF', 'VARCHAR(255)'),
        ('EXPECTEDRETQTY', 'FLOAT'),
        ('INVENTDIMID', 'VARCHAR(255)'),
        ('INVENTTRANSIDRETURN', 'VARCHAR(255)'),
        ('LINEAMOUNT', 'FLOAT'),
        ('LINENUM', 'INT'),
        ('QTYORDERED', 'FLOAT'),
        ('REMAININVENTPHYSICAL', 'FLOAT'),
        ('REMAINSALESPHYSICAL', 'FLOAT'),
        ('RETAILVARIANTID', 'VARCHAR(255)'),
        ('RETURNDISPOSITIONCODEID', 'VARCHAR(50)'),
        ('SALESPRICE', 'FLOAT'),
        ('SHIPPINGDATECONFIRMED', 'DATE'),
        ('SHIPPINGDATEREQUESTED', 'DATE'),
        ('SYSTEMENTRYSOURCE', 'INT'),
        ('LINECREATIONSEQUENCENUMBER', 'INT'),
        ('SALESSALESORDERCREATIONMETHOD', 'VARCHAR(50)'),
        ('DELIVERYPOSTALADDRESS_FK_VALIDFROM', 'DATE'),
        ('INVENTTRANSORIGIN_INVENTTRANSID', 'VARCHAR(255)'),
        ('INVENTTRANSORIGIN_ITEMID', 'VARCHAR(255)'),
        ('INVENTTRANSORIGIN_ITEMINVENTDIMID', 'VARCHAR(255)'),
        ('INVENTTRANSORIGIN_REFERENCEID', 'VARCHAR(255)'),
        ('INVENTTRANSRECID', 'VARCHAR(255)'),
        ('INVENTTRANSORIGIN', 'VARCHAR(50)'),
        ('COSTAMOUNTADJUSTMENT', 'FLOAT'),
        ('COSTAMOUNTPOSTED', 'FLOAT'),
        ('SALESLINECOST', 'FLOAT'),
        ('PARTITION', 'VARCHAR(50)'),
        ('CUSTOMERSLINENUMBER', 'VARCHAR(255)'),
        ('DELIVERYADDRESSNAME', 'VARCHAR(255)'),
        ('DELIVERYMODECODE', 'VARCHAR(50)'),
        ('DELIVERYTERMSID', 'VARCHAR(50)'),
        ('EXTERNALITEMNUMBER', 'VARCHAR(255)'),
        ('INVENTORYLOTID', 'VARCHAR(255)'),
        ('ITEMNUMBER', 'VARCHAR(255)'),
        ('LINEDISCOUNTAMOUNT', 'FLOAT'),
        ('LINEDESCRIPTION', 'VARCHAR(500)'),
        ('SALESPRICEQUANTITY', 'FLOAT'),
        ('CUSTOMERREQUISITIONNUMBER', 'VARCHAR(255)'),
        ('CONFIRMEDRECEIPTDATE', 'DATE'),
        ('REQUESTEDRECEIPTDATE', 'DATE'),
        ('INVENTORYRESERVATIONMETHOD', 'INT'),
        ('SALESORDERNUMBER', 'VARCHAR(255)'),
        ('ORDEREDSALESQUANTITY', 'FLOAT'),
        ('SALESORDERLINESTATUS', 'INT'),
        ('SALESUNITSYMBOL', 'VARCHAR(50)'),
        ('SALESTAXGROUPCODE', 'VARCHAR(50)'),
        ('SALESTAXITEMGROUPCODE', 'VARCHAR(50)'),
        ('SALESPRODUCTCATEGORYNAME', 'VARCHAR(255)'),
        ('DEFAULTLEDGERDIMENSIONDISPLAYVALUE', 'VARCHAR(255)'),
        ('MAINACCOUNTIDDISPLAYVALUE', 'VARCHAR(255)'),
        ('PRODUCTCONFIGURATIONID', 'VARCHAR(255)'),
        ('PRODUCTCOLORID', 'VARCHAR(255)'),
        ('SHIPPINGWAREHOUSEID', 'VARCHAR(255)'),
        ('SHIPPINGSITEID', 'VARCHAR(255)'),
        ('PRODUCTSIZEID', 'VARCHAR(255)'),
        ('ORDEREDINVENTORYSTATUSID', 'VARCHAR(255)'),
        ('PRODUCTSTYLEID', 'VARCHAR(255)'),
        ('SHIPPINGWAREHOUSELOCATIONID', 'VARCHAR(255)'),
        ('RETAILCALCULATEDPERIODICDISCOUNTPERCENTAGE', 'FLOAT'),
        ('RETAILCALCULATEDPERIODICDISCOUNTAMOUNT', 'FLOAT'),
        ('RETAILCALCULATEDTOTALDISCOUNTAMOUNT', 'FLOAT'),
        ('RETAILCALCULATEDTOTALDISCOUNTPERCENTAGE', 'FLOAT'),
        ('RETAILCALCULATEDTENDERDISCOUNTAMOUNT', 'FLOAT'),
        ('FULFILLMENTSTATUS', 'INT'),
        ('FULFILLMENTSTOREID', 'VARCHAR(255)'),
        ('SALESPRODUCTCATEGORYHIERARCHYRECID', 'VARCHAR(255)'),
        ('DELIVERYPOSTALADDRESSRECID', 'VARCHAR(255)'),
        ('FORMATTEDDELVERYADDRESS', 'VARCHAR(500)'),
        ('DELIVERYADDRESSCITY', 'VARCHAR(255)'),
        ('DELIVERYADDRESSCOUNTRYREGIONID', 'VARCHAR(50)'),
        ('DELIVERYADDRESSCOUNTRYREGIONISOCODE', 'VARCHAR(50)'),
        ('DELIVERYADDRESSDESCRIPTION', 'VARCHAR(500)'),
        ('DELIVERYADDRESSDISTRICTNAME', 'VARCHAR(255)'),
        ('ISDELIVERYADDRESSPRIVATE', 'INT'),
        ('DELIVERYADDRESSLATITUDE', 'FLOAT'),
        ('DELIVERYADDRESSLOCATIONID', 'VARCHAR(255)'),
        ('DELIVERYADDRESSLONGITUDE', 'FLOAT'),
        ('DELIVERYADDRESSSTATEID', 'VARCHAR(50)'),
        ('DELIVERYADDRESSSTREET', 'VARCHAR(500)'),
        ('DELIVERYVALIDFROM', 'DATE'),
        ('DELIVERYVALIDTO', 'DATE'),
        ('CREDITNOTEREASONCODE', 'VARCHAR(50)'),
        ('SYNCSTARTDATETIME', 'TIMESTAMP')
    ]
}
