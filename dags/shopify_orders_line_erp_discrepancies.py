from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
import os
import pandas as pd
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Definir la conexión de Snowflake desde variables de entorno
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')

# Lista de destinatarios del correo
mails = ['josefa.gonzalez@patagonia.com', 'jofigonzalez@gmail.com']

# Función que ejecuta la query y envía correo si hay resultados
def check_discrepancies_and_send_email(**kwargs):
    # Usar SnowflakeHook para obtener un objeto de conexión
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Query que detecta discrepancias entre Shopify y ERP
        query = '''
        SELECT 
            shop.ORDER_ID,
            shop.ORDER_NAME,
            shop.total_cantidad_SHOPIFY,
            erp.total_cantidad_ERP
        FROM
            (SELECT 
                s.ORDER_ID,
                s.ORDER_NAME,
                SUM(s.QUANTITY) AS total_cantidad_SHOPIFY
             FROM 
                PATAGONIA.CORE_TEST.SHOPIFY_ORDERS_LINE s
             GROUP BY 
                s.ORDER_ID,
                s.ORDER_NAME) AS shop
        LEFT JOIN
            (SELECT 
                s.PURCHORDERFORMNUM,
                SUM(s.QTY) AS total_cantidad_ERP
             FROM 
                PATAGONIA.CORE_TEST.ERP_PROCESSED_SALESLINE s
             WHERE 
                s.ITEMID != 'DESPACHO'
             GROUP BY 
                s.PURCHORDERFORMNUM) AS erp
        ON 
            shop.ORDER_NAME = TRY_TO_NUMBER(erp.PURCHORDERFORMNUM)
        WHERE 
            shop.total_cantidad_SHOPIFY != erp.total_cantidad_ERP;
        '''
        cursor.execute(query)

        # Obtener los datos y convertir a DataFrame
        data = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df = pd.DataFrame(data, columns=columns)

    finally:
        cursor.close()
        conn.close()

    # Si hay resultados, enviar un correo de alerta
    if not df.empty:
        df_html = df.to_html(index=False)

        # Configurar el correo
        email = EmailOperator(
            task_id='send_email',
            to=mails,
            subject='ALERTA: Discrepancias detectadas entre Shopify y ERP',
            html_content=f"""
                <p>Se han encontrado discrepancias entre Shopify y ERP. A continuación se muestran las órdenes con discrepancias:</p>
                {df_html}
            """,
        )
        email.execute(context=kwargs)
    else:
        print("No se encontraron discrepancias.")

# Definir el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'shopify_orders_line_erp_discrepancies',  # Nombre del DAG
    default_args=default_args,
    description='DAG para detectar discrepancias entre Shopify y ERP en Snowflake',
    schedule_interval='0 0 * * *',  # Ejecutar una vez al día a medianoche
    start_date=days_ago(1),
    catchup=False,
)

# Definir la tarea que ejecutará la query y enviará el correo si hay discrepancias
t1 = PythonOperator(
    task_id='check_discrepancies_and_send_email',
    python_callable=check_discrepancies_and_send_email,
    dag=dag,
)
