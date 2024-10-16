from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
import os
import pandas as pd
from dotenv import load_dotenv
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

# Cargar variables de entorno
load_dotenv()

# Definir la conexión de Snowflake desde variables de entorno
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')

# Definir parámetros del DAG
interval_days = 30 
number_results = 5
max_days = 7  # Máximo número de días para la condición de antigüedad
emails = ['josefa.gonzalez@patagonia.com', 'jofigonzalez@gmail.com']

# Función combinada que verifica OMS-ERP y Shopify-ERP
def check_discrepancies_and_send_combined_email(interval_days, number_results, max_days, **kwargs):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Verificar OMS vs ERP
    start_date = (datetime.now() - timedelta(days=interval_days)).strftime('%Y-%m-%d')
    query_oms = f'''
    SELECT s.*
    FROM PATAGONIA.CORE_TEST.OMS_SUBORDERS s
    LEFT JOIN PATAGONIA.CORE_TEST.ERP_PROCESSED_SALESLINE e
    ON s.ECOMMERCE_NAME = e.PURCHORDERFORMNUM AND e.SALESPOOLID LIKE 'ECOM'
    WHERE e.PURCHORDERFORMNUM IS NULL
    AND s.ORDER_DATE >= '{start_date}'
    '''
    cursor.execute(query_oms)
    columns_oms = [col[0] for col in cursor.description]
    data_oms = cursor.fetchall()
    df_oms = pd.DataFrame(data_oms, columns=columns_oms)
    df_oms.rename(columns={"ECOMMERCE_NAME": "Número de orden", "ORDER_DATE": "Fecha de creación", "ORDER_ID": "ID de orden"}, inplace=True)
    df_oms['Número de orden'] = df_oms.apply(lambda row: f'<a href="https://patagonia.omni.pro/orders/esaleorder/{row["ID de orden"]}" target="_blank">{row["Número de orden"]}</a>', axis=1)
    df_oms_sorted = df_oms.sort_values(by='Fecha de creación', ascending=True)
    df_oms_filtered = df_oms_sorted[['Número de orden', 'Fecha de creación']]

    # Verificar Shopify vs ERP
    query_shopify = f'''
    SELECT s.*
    FROM PATAGONIA.CORE_TEST.SHOPIFY_ORDERS_COPY s
    LEFT JOIN PATAGONIA.CORE_TEST.ERP_PROCESSED_SALESLINE_COPY e
    ON s.NAME = e.PURCHORDERFORMNUM AND e.SALESPOOLID LIKE 'ECOM'
    WHERE e.PURCHORDERFORMNUM IS NULL
      AND s.PROCESSED_AT > CURRENT_TIMESTAMP - INTERVAL '{interval_days} DAYS'
      AND s.FINANCIAL_STATUS = 'paid'
    '''
    cursor.execute(query_shopify)
    columns_shopify = [col[0] for col in cursor.description]
    data_shopify = cursor.fetchall()
    df_shopify = pd.DataFrame(data_shopify, columns=columns_shopify)
    cursor.close()
    conn.close()
    
    df_shopify.rename(columns={"NAME": "Número de orden", "CREATED_AT": "Fecha de creación", "ORDER_ID": "ID de orden"}, inplace=True)
    
    # Hacer clickeable el número de orden, como se hace con OMS-ERP
    df_shopify['Número de orden'] = df_shopify.apply(lambda row: f'<a href="https://admin.shopify.com/store/patagoniachile/orders/{row["ID de orden"]}" target="_blank">{row["Número de orden"]}</a>', axis=1)
    
    df_shopify_sorted = df_shopify.sort_values(by='Fecha de creación', ascending=True)
    df_shopify_filtered = df_shopify_sorted[['Número de orden', 'Fecha de creación']]

    # Calcular la antigüedad de las órdenes más antiguas
    oldest_oms_time = df_oms_filtered['Fecha de creación'].min() if not df_oms_filtered.empty else None
    oldest_shopify_time = df_shopify_filtered['Fecha de creación'].min() if not df_shopify_filtered.empty else None

    time_diff_oms = datetime.now() - pd.to_datetime(oldest_oms_time) if oldest_oms_time else None
    time_diff_shopify = datetime.now() - pd.to_datetime(oldest_shopify_time) if oldest_shopify_time else None

    # Mensajes de antigüedad
    time_message_oms = f"La orden más antigua en OMS es de hace {time_diff_oms.days} días." if time_diff_oms else "No hay órdenes en OMS."
    time_message_shopify = f"La orden más antigua en Shopify es de hace {time_diff_shopify.days} días." if time_diff_shopify else "No hay órdenes en Shopify."

    # Condiciones de alerta
    alert_due_to_age = (time_diff_oms and time_diff_oms.days > max_days) or (time_diff_shopify and time_diff_shopify.days > max_days)
    alert_due_to_number = len(df_oms_filtered) > number_results or len(df_shopify_filtered) > number_results

    # Convertir DataFrames a HTML
    df_oms_html = df_oms_filtered.to_html(index=False, escape=False) if not df_oms_filtered.empty else "<p>No se encontraron discrepancias en OMS.</p>"
    df_shopify_html = df_shopify_filtered.to_html(index=False, escape=False) if not df_shopify_filtered.empty else "<p>No se encontraron discrepancias en Shopify.</p>"

    # Enviar un solo correo si hay alertas
    if alert_due_to_age or alert_due_to_number:
        email_content = f"""
        <p>Se han encontrado discrepancias entre OMS y ERP o Shopify y ERP en los últimos {interval_days} días.</p>
        
        <h3>Discrepancias entre OMS y ERP:</h3>
        {df_oms_html}
        <p>{time_message_oms}</p>
        
        <h3>Discrepancias entre Shopify y ERP:</h3>
        {df_shopify_html}
        <p>{time_message_shopify}</p>
        """
        
        email = EmailOperator(
            task_id='send_combined_email',
            to= emails,
            subject=f'ALERTA: Discrepancias detectadas entre OMS, Shopify y ERP',
            html_content=email_content,
            dag=kwargs['dag']
        )
        email.execute(context=kwargs)
    else:
        print("No se encontraron discrepancias significativas en OMS ni en Shopify.")

# Definir el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'combined_oms_shopify_erp_discrepancies',  # Nombre del DAG combinado
    default_args=default_args,
    description='DAG para ver discrepancias entre OMS, Shopify y ERP en Snowflake',
    schedule_interval='0 0 * * *',  # Ejecutar una vez al día a medianoche
    start_date=days_ago(1),
    catchup=False,
)

# Definir la tarea
t1 = PythonOperator(
    task_id='check_discrepancies_and_send_combined_email',
    python_callable=check_discrepancies_and_send_combined_email,
    op_kwargs={'interval_days': interval_days, 'number_results': number_results, 'max_days': max_days},
    provide_context=True,
    dag=dag,
)
