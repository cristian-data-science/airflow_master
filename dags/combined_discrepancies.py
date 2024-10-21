from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables from .env file
load_dotenv()

SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')

# Parameters to set to trigger the alert
INTERVAL_DAYS = 60  # number of days within the range
NUMBER_RESULTS = 1  # maximum number of results returned
MAX_DAYS = 7  # maximum number of days
EMAILS = [
    'josefa.gonzalez@patagonia.com',
    'jofigonzalez@gmail.com'
]


def check_discrepancies_and_send_combined_email(
    interval_days, number_results, max_days, **kwargs
):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    start_date = (
        datetime.now() - timedelta(days=interval_days)).strftime('%Y-%m-%d')
    query_oms = (
        "SELECT s.* "
        "FROM PATAGONIA.CORE_TEST.OMS_SUBORDERS s "
        "LEFT JOIN PATAGONIA.CORE_TEST.ERP_PROCESSED_SALESLINE e "
        "ON s.ECOMMERCE_NAME = e.PURCHORDERFORMNUM "
        "AND e.SALESPOOLID LIKE 'ECOM' "
        f"WHERE e.PURCHORDERFORMNUM IS NULL AND s.ORDER_DATE >= '{start_date}'"
    )
    cursor.execute(query_oms)
    columns_oms = [col[0] for col in cursor.description]
    data_oms = cursor.fetchall()
    df_oms = pd.DataFrame(data_oms, columns=columns_oms)
    df_oms.rename(
        columns={
            "ECOMMERCE_NAME": "Número de orden",
            "ORDER_DATE": "Fecha de creación",
            "ORDER_ID": "ID de orden"
        }, inplace=True
    )
    df_oms['Número de orden'] = df_oms.apply(
        lambda row: (
            f'<a href="https://patagonia.omni.pro/orders/esaleorder/'
            f'{row["ID de orden"]}" target="_blank">'
            f'{row["Número de orden"]}</a>'
        ),
        axis=1
    )
    df_oms_filtered = df_oms[['Número de orden', 'Fecha de creación']]

    query_shopify = (
        "SELECT s.* "
        "FROM PATAGONIA.CORE_TEST.SHOPIFY_ORDERS_COPY s "
        "LEFT JOIN PATAGONIA.CORE_TEST.ERP_PROCESSED_SALESLINE_COPY e "
        "ON s.NAME = e.PURCHORDERFORMNUM AND e.SALESPOOLID LIKE 'ECOM' "
        f"WHERE e.PURCHORDERFORMNUM IS NULL "
        f"AND s.PROCESSED_AT > CURRENT_TIMESTAMP - INTERVAL '{interval_days} DAYS' "
        "AND s.FINANCIAL_STATUS = 'paid'"
    )
    cursor.execute(query_shopify)
    columns_shopify = [col[0] for col in cursor.description]
    data_shopify = cursor.fetchall()
    df_shopify = pd.DataFrame(data_shopify, columns=columns_shopify)
    df_shopify.rename(
        columns={
            "NAME": "Número de orden",
            "CREATED_AT": "Fecha de creación",
            "ORDER_ID": "ID de orden"
        }, inplace=True
    )
    df_shopify['Número de orden'] = df_shopify.apply(
        lambda row: (
            f'<a href="https://admin.shopify.com/store/patagoniachile/orders/'
            f'{row["ID de orden"]}" target="_blank">'
            f'{row["Número de orden"]}</a>'
        ),
        axis=1
    )
    df_shopify_filtered = df_shopify[['Número de orden', 'Fecha de creación']]

    query_quantity_discrepancy = (
        "SELECT shop.ORDER_ID, shop.ORDER_NAME, shop.total_cantidad_SHOPIFY, "
        "CAST(erp.total_cantidad_ERP AS INTEGER) AS total_cantidad_ERP "
        "FROM (SELECT s.ORDER_ID, s.ORDER_NAME, SUM(s.QUANTITY) AS total_cantidad_SHOPIFY "
        "FROM PATAGONIA.CORE_TEST.SHOPIFY_ORDERS_LINE s "
        "GROUP BY s.ORDER_ID, s.ORDER_NAME) AS shop "
        "LEFT JOIN (SELECT s.PURCHORDERFORMNUM, SUM(s.QTY) AS total_cantidad_ERP "
        "FROM PATAGONIA.CORE_TEST.ERP_PROCESSED_SALESLINE s "
        "WHERE s.ITEMID != 'DESPACHO' "
        "GROUP BY s.PURCHORDERFORMNUM) AS erp "
        "ON shop.ORDER_NAME = TRY_TO_NUMBER(erp.PURCHORDERFORMNUM) "
        "WHERE shop.total_cantidad_SHOPIFY != CAST(erp.total_cantidad_ERP AS INTEGER);"
    )
    cursor.execute(query_quantity_discrepancy)
    columns_quantity = [col[0] for col in cursor.description]
    data_quantity = cursor.fetchall()
    df_quantity = pd.DataFrame(data_quantity, columns=columns_quantity)
    df_quantity.rename(columns={"ORDER_NAME": "Número de orden"}, inplace=True)
    df_quantity['Número de orden'] = df_quantity.apply(
        lambda row: (
            f'<a href="https://admin.shopify.com/store/patagoniachile/orders/'
            f'{row["ORDER_ID"]}" target="_blank">'
            f'{row["Número de orden"]}</a>'
        ),
        axis=1
    )

    cursor.close()
    conn.close()

    df_oms_html = (
        df_oms_filtered.to_html(index=False, escape=False)
        if not df_oms_filtered.empty else (
             "<p>No se encontraron discrepancias en OMS.</p>")
    )
    df_shopify_html = (
        df_shopify_filtered.to_html(index=False, escape=False)
        if not df_shopify_filtered.empty else (  
             "<p>No se encontraron discrepancias en Shopify.</p>")
    )
    df_quantity_html = (
        df_quantity.to_html(index=False, escape=False)
        if not df_quantity.empty else (
            "<p>No se encontraron discrepancias en cantidades"
            "entre Shopify y ERP.</p>"
        )
    )

    if (
         not df_oms_filtered.empty 
         or not df_shopify_filtered.empty 
         or not df_quantity.empty
         ):
        email_content = (
            "<p>Hay órdenes con discrepancias entre OMS, Shopify y ERP:</p>"
            "<h3>Están en OMS pero no en ERP:</h3>"
            f"{df_oms_html}"
            "<h3>Están en Shopify pero no en ERP:</h3>"
            f"{df_shopify_html}"
            "<h3>Discrepancias de cantidades en los ítems de las órdenes "
            "entre Shopify y ERP:</h3>"
            f"{df_quantity_html}"
        )
        email = EmailOperator(
            task_id='send_combined_email',
            to=EMAILS,
            subject=(
                'ALERTA: Discrepancias detectadas entre OMS, Shopify y ERP'),
            html_content=email_content,
            dag=kwargs['dag']
        )
        email.execute(context=kwargs)
    else:
        print(
            "No se encontraron discrepancias significativas en OMS, Shopify"
            "o ERP.")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'combined_oms_erp_shopify_discrepancies',
    default_args=default_args,
    description=(
        'DAG para ver discrepancias entre OMS, Shopify y ERP en Snowflake'),
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    catchup=False,
)

t1 = PythonOperator(
    task_id='check_discrepancies_and_send_combined_email',
    python_callable=check_discrepancies_and_send_combined_email,
    op_kwargs={
        'interval_days': INTERVAL_DAYS,
        'number_results': NUMBER_RESULTS,
        'max_days': MAX_DAYS
    },
    provide_context=True,
    dag=dag,
)
