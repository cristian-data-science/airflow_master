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
interval_days = 100
number_results = 5
max_days = 7  # Máximo número de días para la condición de antigüedad
mails = ['josefa.gonzalez@patagonia.com', 'jofigonzalez@gmail.com']

# Definir la función que se va a ejecutar
def check_discrepancies_and_send_email(interval_days, number_results, max_days, **kwargs):
    # Usar SnowflakeHook para obtener un objeto de conexión
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Query a ejecutar - con filtro de intervalo de días
    query = f'''
    SELECT s.*
    FROM PATAGONIA.CORE_TEST.SHOPIFY_ORDERS_COPY s
    LEFT JOIN PATAGONIA.CORE_TEST.OMS_SUBORDERS e
    ON s.NAME = e.ECOMMERCE_NAME
    WHERE e.ECOMMERCE_NAME IS NULL 
      AND s.PROCESSED_AT > CURRENT_TIMESTAMP - INTERVAL '{interval_days} DAYS'
      AND s.FINANCIAL_STATUS = 'paid'
    '''

    cursor.execute(query)

    # Obtener nombres de columnas
    columns = [col[0] for col in cursor.description]

    # Obtener datos y crear DataFrame
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)

    cursor.close()
    conn.close()

# Renombrar columnas
    df.rename(columns={"NAME": "Número de orden", "CREATED_AT": "Fecha de creación", "ORDER_ID": "ID de orden"}, inplace=True)

# Crear la URL clickeable usando el "ORDER_ID" pero mostrando el "NAME"
    df['Número de orden'] = df.apply(
    lambda row: f'<a href="https://admin.shopify.com/store/patagoniachile/orders/{row["ID de orden"]}" target="_blank">{row["Número de orden"]}</a>',
    axis=1
    )
# Ordenar el DataFrame completo por "Fecha de creación" de la más antigua a la más nueva
    df_sorted_complete = df.sort_values(by='Fecha de creación', ascending=True)

# Filtrar solo las columnas "Número de orden" y "Fecha de creación"
    df_filtered = df_sorted_complete[['Número de orden', 'Fecha de creación']]


    
    # Calcular el tiempo desde la orden más antigua
    oldest_order_time = df_filtered['Fecha de creación'].min()
    time_diff = datetime.now() - pd.to_datetime(oldest_order_time)
    days, seconds = time_diff.days, time_diff.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    time_message = f"La orden más antigua es de hace {days} días, {hours} horas y {minutes} minutos."

    # Condiciones de alerta
    alert_due_to_age = days > max_days
    alert_due_to_number = len(df_filtered) > number_results
# Convertir el DataFrame filtrado y ordenado a HTML
    df_sorted_html = df_filtered.to_html(index=False, escape=False)  # escape=False para que los links se rendericen correctamente en HTML

    # Convertir el DataFrame completo ordenado a HTML
    df_complete_html = df_sorted_complete.to_html(index=False, escape=False)

    num_filas = len(df_filtered)
    # Imprimir alerta y enviar email si se cumple alguna de las dos condiciones
    if alert_due_to_age or alert_due_to_number:
        print(f"ALERTA: Se ha cumplido una o más condiciones de alerta en los últimos {interval_days} días")
        print(df_filtered.head())
        
        # Configurar parámetros del email
        email = EmailOperator(
            task_id='send_email',
            to= mails,
            subject=f'ALERTA: Discrepancias entre OMS y Shopify',
            html_content=f"""
                <p>Se encontraron órdenes que están en Shopify pero no están en OMS:</p>
                <p>{num_filas} órdenes encontradas.</p>
                 <p>{time_message}</p>
                
                {df_sorted_html}
               
            """,
            dag=kwargs['dag']
        )
        email.execute(context=kwargs)
    else:
        print(f"No se encontraron discrepancias que cumplan con las condiciones de alerta en los últimos {interval_days} días.")

# Definir el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'oms_shopify_discrepancies',  # Nombre del DAG
    default_args=default_args,
    description='DAG para ver discrepancias entre OMS y Shopify en Snowflake',
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
