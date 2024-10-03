from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
import os
import pandas as pd
from dotenv import load_dotenv
from airflow.operators.email import EmailOperator
from datetime import datetime

# Cargar variables de entorno
load_dotenv()

# Definir la conexión de Snowflake desde variables de entorno
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')

# Definir parámetros del DAG
interval_days = 30  
number_results = 5
max_days = 7  # Máximo número de días para la condición de antigüedad
emails = ['josefa.gonzalez@patagonia.com', 'jofigonzalez@gmail.com']

# Definir la función que se va a ejecutar
def check_discrepancies_and_send_email(interval_days, number_results, max_days, **kwargs):
    # Usar SnowflakeHook para obtener un objeto de conexión
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Primera consulta: Discrepancias entre OMS y ERP
    query_oms_erp = '''
    SELECT s.*
    FROM PATAGONIA.CORE_TEST.OMS_SUBORDERS s
    LEFT JOIN PATAGONIA.CORE_TEST.ERP_PROCESSED_SALESLINE e
    ON s.ECOMMERCE_NAME = e.PURCHORDERFORMNUM AND e.SALESPOOLID LIKE 'ECOM'
    WHERE e.PURCHORDERFORMNUM IS NULL
    '''

    # Ejecutar la consulta y crear DataFrame
    cursor.execute(query_oms_erp)
    columns = [col[0] for col in cursor.description]
    data = cursor.fetchall()
    df_oms_erp = pd.DataFrame(data, columns=columns)
    
    # Renombrar columnas
    df_oms_erp.rename(columns={"ECOMMERCE_NAME": "Número de orden", "ORDER_DATE": "Fecha de creación", "ORDER_ID": "ID de orden"}, inplace=True)
    
    # Crear la URL clickeable
    df_oms_erp['Número de orden'] = df_oms_erp.apply(
        lambda row: f'<a href="https://patagonia.omni.pro/orders/esaleorder/{row["ID de orden"]}" target="_blank">{row["Número de orden"]}</a>',
        axis=1
    )
    
    # Ordenar y filtrar columnas
    df_oms_erp_sorted = df_oms_erp.sort_values(by='Fecha de creación', ascending=True)
    df_oms_erp_filtered = df_oms_erp_sorted[['Número de orden', 'Fecha de creación']]
    
    # Segunda consulta: Discrepancias entre ERP y Shopify
    query_erp_shopify = f'''
    SELECT s.*
    FROM PATAGONIA.CORE_TEST.SHOPIFY_ORDERS_COPY s
    LEFT JOIN PATAGONIA.CORE_TEST.ERP_PROCESSED_SALESLINE_COPY e
    ON s.NAME = e.PURCHORDERFORMNUM AND e.SALESPOOLID LIKE 'ECOM'
    WHERE e.PURCHORDERFORMNUM IS NULL
      AND s.PROCESSED_AT > CURRENT_TIMESTAMP - INTERVAL '{interval_days} DAYS'
      AND s.FINANCIAL_STATUS = 'paid'
    '''
    
    # Ejecutar la consulta y crear DataFrame
    cursor.execute(query_erp_shopify)
    columns = [col[0] for col in cursor.description]
    data = cursor.fetchall()
    df_erp_shopify = pd.DataFrame(data, columns=columns)
    
    # Renombrar columnas
    df_erp_shopify.rename(columns={"NAME": "Número de orden", "CREATED_AT": "Fecha de creación", "ORDER_ID": "ID de orden"}, inplace=True)
    
    # Crear la URL clickeable
    df_erp_shopify['Número de orden'] = df_erp_shopify.apply(
        lambda row: f'<a href="https://admin.shopify.com/store/patagoniachile/orders/{row["ID de orden"]}" target="_blank">{row["Número de orden"]}</a>',
        axis=1
    )
    
    # Ordenar y filtrar columnas
    df_erp_shopify_sorted = df_erp_shopify.sort_values(by='Fecha de creación', ascending=True)
    df_erp_shopify_filtered = df_erp_shopify_sorted[['Número de orden', 'Fecha de creación']]
    
    cursor.close()
    conn.close()
    
    # Convertir los DataFrames a HTML
    df_oms_erp_html = df_oms_erp_filtered.to_html(index=False, escape=False)
    df_erp_shopify_html = df_erp_shopify_filtered.to_html(index=False, escape=False)
    
    # Calcular el tiempo desde la orden más antigua en ambas tablas
    oldest_order_time_oms_erp = df_oms_erp_filtered['Fecha de creación'].min()
    time_diff_oms_erp = datetime.now() - pd.to_datetime(oldest_order_time_oms_erp)
    days_oms_erp, seconds_oms_erp = time_diff_oms_erp.days, time_diff_oms_erp.seconds
    hours_oms_erp = seconds_oms_erp // 3600
    time_message_oms_erp = f"La orden más antigua (OMS-ERP) es de hace {days_oms_erp} días y {hours_oms_erp} horas."

    oldest_order_time_erp_shopify = df_erp_shopify_filtered['Fecha de creación'].min()
    time_diff_erp_shopify = datetime.now() - pd.to_datetime(oldest_order_time_erp_shopify)
    days_erp_shopify, seconds_erp_shopify = time_diff_erp_shopify.days, time_diff_erp_shopify.seconds
    hours_erp_shopify = seconds_erp_shopify // 3600
    time_message_erp_shopify = f"La orden más antigua (ERP-Shopify) es de hace {days_erp_shopify} días y {hours_erp_shopify} horas."

    # Enviar email si se cumple alguna de las dos condiciones
    if (days_oms_erp > max_days or len(df_oms_erp_filtered) > number_results) or (days_erp_shopify > max_days or len(df_erp_shopify_filtered) > number_results):
        email = EmailOperator(
            task_id='send_email',
            to=emails,
            subject='ALERTA: Discrepancias encontradas entre OMS/ERP y ERP/Shopify',
            html_content=f"""
                <h3>Discrepancias entre OMS y ERP:</h3>
                <p>{len(df_oms_erp_filtered)} órdenes encontradas.</p>
                <p>{time_message_oms_erp}</p>
                {df_oms_erp_html}

                <h3>Discrepancias entre ERP y Shopify:</h3>
                <p>{len(df_erp_shopify_filtered)} órdenes encontradas.</p>
                <p>{time_message_erp_shopify}</p>
                {df_erp_shopify_html}
            """,
            dag=kwargs['dag']
        )
        email.execute(context=kwargs)
    else:
        print("No se encontraron discrepancias que cumplan con las condiciones de alerta.")

# Definir el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'combined_discrepancies',  # Nombre del DAG
    default_args=default_args,
    description='DAG para ver discrepancias combinadas entre OMS/ERP y ERP/Shopify en Snowflake',
    schedule_interval='0 0 * * *',  # Ejecutar una vez al día a medianoche
    start_date=days_ago(1),
    catchup=False,
)

# Pasar los parámetros a la tarea
t1 = PythonOperator(
    task_id='check_discrepancies_and_send_email',
    python_callable=check_discrepancies_and_send_email,
    op_kwargs={'interval_days': interval_days, 'number_results': number_results, 'max_days': max_days},
    provide_context=True,
    dag=dag,
)