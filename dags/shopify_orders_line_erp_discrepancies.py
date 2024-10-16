from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
import os
import pandas as pd
from dotenv import load_dotenv


load_dotenv()

SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')

#email addresses to send 
mails = ['josefa.gonzalez@patagonia.com', 'jofigonzalez@gmail.com']


def check_discrepancies_and_send_email(**kwargs):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        
        query = '''
        SELECT 
            shop.ORDER_ID,
            shop.ORDER_NAME,
            shop.total_cantidad_SHOPIFY,
            CAST(erp.total_cantidad_ERP AS INTEGER) AS total_cantidad_ERP
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
            shop.total_cantidad_SHOPIFY != CAST(erp.total_cantidad_ERP AS INTEGER);
        '''
        cursor.execute(query)

       
        data = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df = pd.DataFrame(data, columns=columns)

        
        df.rename(columns={"ORDER_NAME": "Número de orden"}, inplace=True)
        df['Número de orden'] = df.apply(
            lambda row: f'<a href="https://admin.shopify.com/store/patagoniachile/orders/{row["ORDER_ID"]}" target="_blank">{row["Número de orden"]}</a>',
            axis=1
        )

        
        df.drop(columns=['ORDER_ID'], inplace=True)

    finally:
        cursor.close()
        conn.close()

    
    if not df.empty:
        df_html = df.to_html(index=False, escape=False)  

        email = EmailOperator(
            task_id='send_email',
            to=mails,
            subject='ALERTA: Discrepancias detectadas entre Shopify y ERP',
            html_content=f"""
                <p>Hay órdenes que muestran distinta cantidad de productos en Shopify y en ERP. A continuación se presentan las órdenes con discrepancias:</p>
                {df_html}
            """,
        )
        email.execute(context=kwargs)
    else:
        print("No se encontraron discrepancias.")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'shopify_orders_line_erp_discrepancies',  
    default_args=default_args,
    description='DAG para detectar discrepancias entre Shopify y ERP en Snowflake',
    schedule_interval='0 0 * * *',  
    start_date=days_ago(1),
    catchup=False,
)


t1 = PythonOperator(
    task_id='check_discrepancies_and_send_email',
    python_callable=check_discrepancies_and_send_email,
    dag=dag,
)
