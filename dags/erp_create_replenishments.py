from airflow import DAG
from airflow.operators.python import PythonOperator
from config.erp_create_replenishments_config import default_args, dag_config
import requests
import pandas as pd
import os
from datetime import datetime
import pytz
import logging
from typing import List, Dict, Any


def get_erp_token() -> str:
    """
    Gets an ERP token.
    Returns:
        The ERP token.
    Raises:
        Exception: If an error occurs.
    """
    erp_token_url = os.environ.get('ERP_TOKEN_URL')
    erp_url = os.environ.get('ERP_URL')
    erp_client_id = os.environ.get('ERP_CLIENT_ID')
    erp_client_secret = os.environ.get('ERP_CLIENT_SECRET')

    if not all([erp_token_url, erp_url, erp_client_id, erp_client_secret]):
        raise ValueError("Missing ERP credentials in environment variables")

    data = {
        'grant_type': 'client_credentials',
        'client_id': erp_client_id,
        'client_secret': erp_client_secret,
        'scope': f"{erp_url}/.default"
    }

    response = requests.post(
        f"{erp_token_url}/oauth2/v2.0/token", data=data)

    if response.status_code != 200:
        raise Exception(
            f"Error getting ERP token: {response.status_code} {response.text}")

    token_data = response.json()
    if not token_data.get('access_token'):
        raise Exception("Error al obtener token ERP")

    return token_data['access_token']


def create_erp_header(token: str, receiving_warehouse_id: str) -> str:
    """
    Creates a header in the ERP for a given transfer order.
    Args:
        token: The ERP token.
        receiving_warehouse_id: The receiving warehouse ID.
    Returns:
        The ERP transfer order number.
    Raises:
        Exception: If an error occurs.
    """
    erp_url = os.environ.get('ERP_URL')
    if not erp_url:
        raise ValueError("Missing ERP_URL in environment variables")

    # Format dates in ISO 8601 format with UTC timezone indicator (Z)
    from datetime import timezone
    now = datetime.now(timezone.utc)
    formatted_date = now.strftime('%Y-%m-%dT%H:%M:%SZ')

    body = {
        "dataAreaId": "pat",
        "RequestedReceiptDate": formatted_date,
        "ShippingWarehouseId": "CD",
        "ReceivingWarehouseId": receiving_warehouse_id,
        "TransferOrderPromisingMethod": "None",
        "AreLinesAutomaticallyReservedByDefault": "Yes",
        "RequestedShippingDate": formatted_date,
        "TransferOrderStockTransferPriceType": "CostPrice"
    }

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    response = requests.post(
        f"{erp_url}/data/TransferOrderHeaders",
        headers=headers, json=body)

    if response.status_code not in (200, 201):
        raise Exception(
            f"Error al crear cabecera para {receiving_warehouse_id}: "
            f"{response.status_code} {response.text}")

    data = response.json()
    return data.get('TransferOrderNumber')


def create_erp_line(
    token: str,
    transfer_order_number: str,
    line_data: Dict[str, Any]
) -> Dict[str, str]:
    """
    Creates a line in the ERP for a given transfer order.
    Args:
        token: The ERP token.
        transfer_order_number: The transfer order number.
        line_data: The line data.
    Returns:
        Dictionary with the ERP line ID.
    Raises:
        Exception: If an error occurs.
    """
    erp_url = os.environ.get('ERP_URL')
    if not erp_url:
        raise ValueError("Missing ERP_URL in environment variables")

    body = {
        "dataAreaId": "pat",
        "TransferOrderNumber": transfer_order_number,
        "LineNumber": line_data['LineNumber'],
        "OrderedInventoryStatusId": line_data['OrderedInventoryStatusId'],
        "ProductStyleId": line_data['ProductStyleId'],
        "TransferQuantity": line_data['TransferQuantity'],
        "RequestedReceiptDate": line_data['RequestedReceiptDate'],
        "RequestedShippingDate": line_data['RequestedShippingDate'],
        "ProductConfigurationId": line_data['ProductConfigurationId'],
        "ProductSizeId": line_data['ProductSizeId'],
        "ProductColorId": line_data['ProductColorId'],
        "ItemNumber": line_data['ItemNumber'],
        "ShippingWarehouseLocationId":
        line_data['ShippingWarehouseLocationId'],
        "SalesTaxItemGroupCodeShipment":
            line_data['SalesTaxItemGroupCodeShipment'],
        "SalesTaxItemGroupCodeReceipt":
            line_data['SalesTaxItemGroupCodeReceipt'],
        "PriceType": line_data['PriceType'],
        # Fixed fields
        "ATPTimeFenceDays": 0,
        "AllowedUnderdeliveryPercentage": 0,
        "WillProductReceivingCrossDockProducts": "No",
        "OverrideFEFODateControl": "No",
        "IntrastatCostAmount": 0,
        "ATPDelayedSupplyOffsetDays": 0,
        "IntrastatStatisticalValue": 0,
        "OverrideSalesTaxShipment": "No",
        "TransferCatchWeightQuantity": 0,
        "PlanningPriority": 0,
        "OverrideSalesTaxReceipt": "No",
        "TransferOrderPromisingMethod": "None",
        "AllowedOverdeliveryPercentage": 0,
        "ATPBackwardSupplyTimeFenceDays": 0,
        "IsAutomaticallyReserved": "Yes",
        "IsATPIncludingPlannedOrders": False,
        "ATPDelayedDemandOffsetDays": 0,
        "InventCostPriceCalculated": 0,
        "MaximumRetailPrice": 0,
        "NetAmount": 0,
        "DefaultDimension": 0,
        "UnitPrice": 0,
        "CurrencyCode": "",
        "AssessableValueTransactionCurrency": 0,
        "InvntCostPrice": 0,
        "Retention": 0,
        "VATPriceType": "CostPrice"
    }

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    response = requests.post(
        f"{erp_url}/data/TransferOrderLines",
        headers=headers, json=body)

    if response.status_code not in (200, 201):
        error_text = response.text
        raise Exception(
            f"Error al crear línea para TR {transfer_order_number}: "
            f"{response.status_code} {error_text}")

    data = response.json()
    return {"ERP_LINE_ID": data.get("ShippingInventoryLotId")}


def get_replenishment_data(replenishment_id: str) -> Dict[str, Any]:
    """
    Get replenishment header data from Snowflake.
    Args:
        replenishment_id: The replenishment ID.
    Returns:
        Dict with replenishment header data.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    snowflake_conn_id = default_args['snowflake_conn_id']
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    query = f"""
    SELECT * FROM PATAGONIA.CORE_TEST.PATCORE_REPLENISHMENTS
    WHERE ID = '{replenishment_id}'
    """

    result = snowflake_hook.get_pandas_df(query)

    if result.empty:
        raise ValueError(f"No replenishment found with ID: {replenishment_id}")
    return result.iloc[0].to_dict()


def get_enriched_lines(replenishment_id: str) -> pd.DataFrame:
    """
    Get enriched replenishment line data from Snowflake.
    Args:
        replenishment_id: The replenishment ID.
    Returns:
        DataFrame with enriched line data.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    snowflake_conn_id = default_args['snowflake_conn_id']
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    # Query with a fallback for SKUs not found in ERP_PRODUCTS
    query = f"""
    SELECT
        ROW_NUMBER()
            OVER (PARTITION BY rpl.STORE ORDER BY rpl.SKU) AS LINENUMBER,
        prod.ITEMNUMBER,
        'DISPONIBLE' AS ORDEREDINVENTORYSTATUSID,
        prod.COLOR AS PRODUCTCOLORID,
        prod.CONFIGURATION AS PRODUCTCONFIGURATIONID,
        prod.SIZE AS PRODUCTSIZEID,
        'GEN' AS PRODUCTSTYLEID,
        'CD' AS SHIPPINGWAREHOUSEID,
        'GENERICA' AS SHIPPINGWAREHOUSELOCATIONID,
        rpl.REPLENISHMENT AS TRANSFERQUANTITY,
        rpl.STORE AS TIENDA,
        rpl.SKU,
        prod.TEAM,
        prod.CATEGORY,
        prod.PRODUCTNAME,
        rpl.DELIVERY,
        rpl.ERP_TR_ID,
        rpl.ERP_LINE_ID,
        CASE WHEN prod.ITEMNUMBER IS NULL THEN TRUE
             ELSE FALSE END AS IS_MISSING_ERP_DATA
    FROM PATAGONIA.CORE_TEST.PATCORE_REPLENISHMENTS_LINE rpl
    LEFT JOIN PATAGONIA.CORE_TEST.ERP_PRODUCTS prod
        ON rpl.SKU = prod.SKU
    WHERE rpl.REPLENISHMENT_ID = '{replenishment_id}'
    ORDER BY rpl.STORE, LINENUMBER
    """

    result = snowflake_hook.get_pandas_df(query)
    return result


def update_erp_info_in_replenishment(
    replenishment_id: str,
    erp_trs: str,
    lines: List[Dict[str, str]]
):
    """
    Update ERP info in replenishment records using a temporary
    table for efficiency.
    Args:
        replenishment_id: The replenishment ID.
        erp_trs: Comma-separated list of ERP transfer IDs.
        lines: List of line info with ERP IDs.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    import uuid

    snowflake_conn_id = default_args['snowflake_conn_id']
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    # Update the header with the ERP TR IDs
    header_query = f"""
    UPDATE PATAGONIA.CORE_TEST.PATCORE_REPLENISHMENTS
    SET ERP_TRS_IDS = '{erp_trs}'
    WHERE ID = '{replenishment_id}'
    """

    snowflake_hook.run(header_query)

    # If no lines to update, just return
    if not lines:
        logging.info("No lines to update in replenishment")
        return

    # Create a unique temporary table name to avoid conflicts
    temp_table_name = f"TEMP_REPLENISHMENT_UPDATE_{uuid.uuid4().hex[:8]}"

    # Use a single connection for all operations
    with snowflake_hook.get_conn() as conn:
        with conn.cursor() as cur:
            # Create temporary table
            create_temp_table_query = f"""
            CREATE TEMPORARY TABLE PATAGONIA.CORE_TEST.{temp_table_name} (
                ERP_TR_ID STRING,
                ERP_LINE_ID STRING,
                REPLENISHMENT_ID STRING,
                SKU STRING,
                STORE STRING
            )
            """
            cur.execute(create_temp_table_query)

            # Insert data into temporary table -
            # do it in batches to avoid query size limits
            batch_size = 1000
            for i in range(0, len(lines), batch_size):
                batch = lines[i:i+batch_size]

                # Build the values part of the INSERT statement
                values_clauses = []
                for line in batch:
                    values_clauses.append(
                        f"('{line['ERP_TR_ID']}', "
                        f"'{line['ERP_LINE_ID']}', "
                        f"'{replenishment_id}', "
                        f"'{line['SKU']}', "
                        f"'{line['STORE']}')"
                    )

                values_str = ",".join(values_clauses)

                insert_query = f"""
                INSERT INTO PATAGONIA.CORE_TEST.{temp_table_name} (
                    ERP_TR_ID, ERP_LINE_ID, REPLENISHMENT_ID, SKU, STORE
                )
                VALUES
                {values_str}
                """
                cur.execute(insert_query)

            # Update replenishment lines with temporary data
            update_query = f"""
            UPDATE PATAGONIA.CORE_TEST.PATCORE_REPLENISHMENTS_LINE l
            SET
                l.ERP_TR_ID = t.ERP_TR_ID,
                l.ERP_LINE_ID = t.ERP_LINE_ID
            FROM PATAGONIA.CORE_TEST.{temp_table_name} t
            WHERE
                l.REPLENISHMENT_ID = t.REPLENISHMENT_ID
                AND l.SKU = t.SKU
                AND l.STORE = t.STORE
            """
            cur.execute(update_query)

            # Drop temporary table
            drop_query = f"DROP TABLE PATAGONIA.CORE_TEST.{temp_table_name}"
            cur.execute(drop_query)

            # Commit the transaction
            conn.commit()

    logging.info("Updated ERP info in replenishment "
                 "successfully with batch update")


def send_replenishment_summary_email(
    replenishment_id: str,
    erp_tr_numbers: List[str],
    lines_created: int,
    missing_skus: List[Dict[str, str]]
):
    """
    Send a summary email about the replenishment process.
    Args:
        replenishment_id: The replenishment ID.
        erp_tr_numbers: List of ERP transfer order numbers created.
        lines_created: Number of lines created.
        missing_skus: List of SKUs that couldn't be created.
    Returns:
        Dictionary containing email subject and HTML content.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    snowflake_conn_id = default_args['snowflake_conn_id']
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    # Get header info
    query = f"""
    SELECT * FROM PATAGONIA.CORE_TEST.PATCORE_REPLENISHMENTS
    WHERE ID = '{replenishment_id}'
    """
    header = snowflake_hook.get_pandas_df(query)

    # Get summary by ERP TR
    query = f"""
    SELECT
        ERP_TR_ID, STORE, COUNT(*) as LINE_COUNT,
        SUM(REPLENISHMENT) as TOTAL_UNITS
    FROM PATAGONIA.CORE_TEST.PATCORE_REPLENISHMENTS_LINE
    WHERE REPLENISHMENT_ID = '{replenishment_id}'
    AND ERP_TR_ID IS NOT NULL
    GROUP BY ERP_TR_ID, STORE
    ORDER BY STORE
    """
    summary = snowflake_hook.get_pandas_df(query)

    chile_tz = pytz.timezone('America/Santiago')
    current_time = datetime.now(chile_tz)
    created_date = current_time.strftime('%Y-%m-%d %H:%M:%S %Z')
    week = f'Week {current_time.isocalendar()[1]}'

    erp_tr_list = ', '.join(erp_tr_numbers) if erp_tr_numbers else 'None'

    # Safely access header values - check if header is not empty first
    if not header.empty:
        stores_considered = (
            header['STORES_CONSIDERED'].values[0]
            if 'STORES_CONSIDERED' in header.columns else 'N/A'
        )
        total_units = (
            header['TOTAL_REPLENISHMENT'].values[0]
            if 'TOTAL_REPLENISHMENT' in header.columns else 'N/A'
        )
        selected_deliveries = (
            header['SELECTED_DELIVERIES'].values[0]
            if 'SELECTED_DELIVERIES' in header.columns else 'N/A'
        )
    else:
        stores_considered = 'N/A'
        total_units = 'N/A'
        selected_deliveries = 'N/A'

    # Prepare summary table
    summary_table = ""
    if not summary.empty:
        summary_table = "<table border='1' cellpadding='5' cellspacing='0'>"
        header_row = (
            "<tr><th>Store</th><th>ERP Transfer</th>"
            "<th>Line Count</th><th>Total Units</th></tr>"
        )
        summary_table += header_row

        for _, row in summary.iterrows():
            summary_table += "<tr>"
            summary_table += f"<td>{row['STORE']}</td>"
            summary_table += f"<td>{row['ERP_TR_ID']}</td>"
            summary_table += f"<td>{row['LINE_COUNT']}</td>"
            summary_table += f"<td>{row['TOTAL_UNITS']}</td>"
            summary_table += "</tr>"

        summary_table += "</table>"
    else:
        summary_table = "<p>No lines were created in ERP</p>"

    # Prepare missing SKUs table
    missing_skus_table = ""

    if missing_skus:
        missing_skus_table = (
            "<table border='1' cellpadding='5' cellspacing='0'>"
        )
        missing_skus_table += (
            "<tr><th>Store</th><th>SKU</th><th>Error</th></tr>"
        )

        for sku in missing_skus:
            missing_skus_table += "<tr>"
            missing_skus_table += f"<td>{sku.get('STORE', 'N/A')}</td>"
            missing_skus_table += f"<td>{sku.get('SKU', 'N/A')}</td>"
            missing_skus_table += f"<td>{sku.get('ERROR', 'Unknown')}</td>"
            missing_skus_table += "</tr>"

        missing_skus_table += "</table>"
    else:
        missing_skus_table = "<p>No missing SKUs</p>"

    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            .header {{ background-color: #f2f2f2; padding: 20px; }}
            .content {{ padding: 20px; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th {{ background-color: #f2f2f2; text-align: left; }}
            td, th {{ padding: 8px; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h2>Replenishment Summary: {replenishment_id}</h2>
            <p><strong>Created Date:</strong> {created_date}</p>
            <p><strong>Week:</strong> {week}</p>
            <p><strong>Stores:</strong> {stores_considered}</p>
            <p><strong>Selected Deliveries:</strong> {selected_deliveries}</p>
            <p><strong>Total units:</strong> {total_units}</p>
            <p><strong>ERP Transfers Created:</strong> {erp_tr_list}</p>
            <p><strong>Lines Created:</strong> {lines_created}</p>
        </div>
        <div class="content">
            <h3>Transfer Orders Summary</h3>
            {summary_table}

            <h3>Missing SKUs</h3>
            {missing_skus_table}
        </div>
    </body>
    </html>
    """

    return {
        'subject': (
            f'Replenishment {replenishment_id} - '
            'ERP Transfer Orders Created'
        ),
        'html_content': html_content
    }


def process_replenishment(**context):
    """
    Main function to process a replenishment and create it in ERP.
    """
    # Get replenishment ID from DAG run configuration
    dag_run = context['dag_run']
    replenishment_id = dag_run.conf.get('replenishmentID')

    if not replenishment_id:
        raise ValueError(
            "replenishmentID not provided in DAG run configuration")

    logging.info(f"Processing replenishment ID: {replenishment_id}")

    # Get replenishment data from Snowflake
    replenishment_data = get_replenishment_data(replenishment_id)
    logging.info(
        f"Retrieved replenishment header data: {replenishment_data}")

    # Get enriched line data
    enriched_lines = get_enriched_lines(replenishment_id)
    logging.info(f"Retrieved {len(enriched_lines)} enriched lines")

    # Check for missing ERP data
    missing_erp_data = enriched_lines[enriched_lines['IS_MISSING_ERP_DATA']]
    if not missing_erp_data.empty:
        missing_skus = missing_erp_data['SKU'].tolist()
        logging.warning(
            f"Found {len(missing_skus)} SKUs without ERP data: {missing_skus}")

    # Filter out lines with missing ERP data
    valid_lines = enriched_lines[~enriched_lines['IS_MISSING_ERP_DATA']]

    if valid_lines.empty:
        raise ValueError(
            "No valid lines with ERP data found, "
            "cannot create ERP replenishments")

    # Group lines by store
    lines_by_store = {}
    for _, line in valid_lines.iterrows():
        store = line['TIENDA']
        if store not in lines_by_store:
            lines_by_store[store] = []
        lines_by_store[store].append(line.to_dict())

    # Get ERP token
    token = get_erp_token()
    logging.info("Retrieved ERP token successfully")

    # Process each store
    erp_tr_numbers = []
    erp_lines_with_info = []

    for store, store_lines in lines_by_store.items():
        logging.info(f"Creating ERP header for store: {store}")
        try:
            # Create TransferHeader for this store
            transfer_order_number = create_erp_header(token, store)
            logging.info(
                f"Created ERP header for store {store}: "
                f"{transfer_order_number}")

            erp_tr_numbers.append(transfer_order_number)

            # Format dates for ERP API - ISO 8601 with UTC timezone
            from datetime import timezone
            now = datetime.now(timezone.utc)
            formatted_date = now.strftime('%Y-%m-%dT%H:%M:%SZ')

            # Create lines for this Transfer Order
            for i, line in enumerate(store_lines):
                line_data = {
                    "ItemNumber": line['ITEMNUMBER'],
                    "ProductColorId": line['PRODUCTCOLORID'],
                    "ProductConfigurationId": line['PRODUCTCONFIGURATIONID'],
                    "ProductSizeId": line['PRODUCTSIZEID'],
                    "ProductStyleId": line['PRODUCTSTYLEID'],
                    "OrderedInventoryStatusId":
                        line['ORDEREDINVENTORYSTATUSID'],
                    "ShippingWarehouseLocationId":
                        line['SHIPPINGWAREHOUSELOCATIONID'],
                    "TransferQuantity": line['TRANSFERQUANTITY'],
                    "RequestedReceiptDate": formatted_date,
                    "RequestedShippingDate": formatted_date,
                    "SalesTaxItemGroupCodeShipment": "IVA",
                    "SalesTaxItemGroupCodeReceipt": "EXENTO",
                    "PriceType": "CostPrice",
                    "LineNumber": i + 1
                }

                logging.info(
                    f"Creating ERP line for {transfer_order_number}, "
                    f"line {i+1}")
                line_result = \
                    create_erp_line(token, transfer_order_number, line_data)

                erp_lines_with_info.append({
                    "SKU": line['SKU'],
                    "STORE": line['TIENDA'],
                    "ERP_TR_ID": transfer_order_number,
                    "ERP_LINE_ID": line_result["ERP_LINE_ID"]
                })

                logging.info(
                    f"Created ERP line for {transfer_order_number}, "
                    f"line {i+1}: {line_result}")

        except Exception as e:
            logging.error(f"Error processing store {store}: {str(e)}")
            raise

    # Update ERP info in Snowflake
    if erp_tr_numbers and erp_lines_with_info:
        erp_trs_str = ', '.join(erp_tr_numbers)
        logging.info(f"Updating replenishment with ERP TRs: {erp_trs_str}")
        update_erp_info_in_replenishment(
            replenishment_id, erp_trs_str, erp_lines_with_info
        )
        logging.info("Updated ERP info in replenishment successfully")

    # Prepare and send email summary
    summary = {
        "replenishment_id": replenishment_id,
        "erp_tr_numbers": erp_tr_numbers,
        "lines_created": len(erp_lines_with_info),
        "missing_skus":
            len(missing_erp_data) if not missing_erp_data.empty else 0
    }

    logging.info("Preparing email summary")
    email_data = send_replenishment_summary_email(
        replenishment_id, erp_tr_numbers,
        len(erp_lines_with_info),
        len(missing_erp_data) if not missing_erp_data.empty else 0
    )

    # Send email
    from airflow.operators.email import EmailOperator

    recipients = (
        dag_config.get('notification_emails') or
        [context['dag_run'].conf.get('email')] or
        [default_args['email']]
    )

    recipients = [email for email in recipients if email]

    if recipients:
        email = EmailOperator(
            task_id='send_email_summary',
            to=recipients,
            subject=email_data['subject'],
            html_content=email_data['html_content'],
            dag=context['dag']
        )
        email.execute(context=context)
        logging.info(f"Email summary sent to: {', '.join(recipients)}")
    else:
        logging.warning("No email recipients configured, "
                        "skipping email notification")

    return summary


# Define the DAG
with DAG(
    dag_config['dag_id'],
    default_args=default_args,
    description=dag_config['description'],
    schedule_interval=dag_config['schedule_interval'],
    catchup=dag_config['catchup'],
    tags=dag_config['tags'],
) as dag:
    process_task = PythonOperator(
        task_id='process_replenishment',
        python_callable=process_replenishment,
        provide_context=True,
    )
