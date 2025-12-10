from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from config.combined_discrepancies_config import default_args
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging

# Load environment variables from .env
load_dotenv()
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')

# Gmail SMTP configuration (from .env)
GMAIL_USER = os.getenv('GMAIL_USER')
GMAIL_PASS = os.getenv('GMAIL_PASS')
EMAIL_FROM = os.getenv('EMAIL_FROM', GMAIL_USER)

# Parameters for triggering the alert
INTERVAL_DAYS = 30  # Days in the query range
NUMBER_RESULTS = 2  # Minimum number of results to trigger an alert
MAX_DAYS = 7  # Maximum number of days since the order was created

# Default emails if not specified in Airflow Variables
DEFAULT_EMAILS = [
    'enrique.urrutia@patagonia.com',
    'cesar.orostegui@patagonia.com',
    'clemente.videla@patagonia.com',
    'nicole.parra@patagonia.com',
    'daniela.delaveau@patagonia.com'
]


def send_email_via_gmail(to_emails, subject, html_content):
    """
    Sends an email using Gmail SMTP.

    Args:
        to_emails: List of recipient email addresses
        subject: Email subject
        html_content: HTML content of the email
    """
    if not GMAIL_USER or not GMAIL_PASS:
        raise ValueError(
            "Gmail credentials not configured. "
            "Please set GMAIL_USER and GMAIL_PASS in .env"
        )

    # Create message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = EMAIL_FROM
    msg['To'] = ', '.join(to_emails)

    # Attach HTML content
    html_part = MIMEText(html_content, 'html', 'utf-8')
    msg.attach(html_part)

    try:
        # Connect to Gmail SMTP server
        logging.info("Connecting to Gmail SMTP server...")
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(GMAIL_USER, GMAIL_PASS)

        # Send email
        server.sendmail(EMAIL_FROM, to_emails, msg.as_string())
        server.quit()

        logging.info(
            f"Email sent successfully via Gmail to {len(to_emails)} recipients"
        )
        return True

    except smtplib.SMTPAuthenticationError as e:
        logging.error(f"Gmail authentication failed: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to send email via Gmail: {e}")
        raise


def send_email(to_emails, subject, html_content, dag, context):
    """
    Sends an email using the configured method (sendgrid or gmail).

    The method is determined by the Airflow Variable 'email_method'.
    Default is 'sendgrid'.
    """
    # Get email method from Airflow Variable
    email_method = Variable.get(
        'email_method', default_var='sendgrid'
    ).lower().strip()

    logging.info(f"Email method configured: {email_method}")

    if email_method == 'gmail':
        # Send via Gmail SMTP
        send_email_via_gmail(to_emails, subject, html_content)
    else:
        # Send via Airflow EmailOperator (SendGrid or configured SMTP)
        email = EmailOperator(
            task_id='send_combined_email',
            to=to_emails,
            subject=subject,
            html_content=html_content,
            dag=dag
        )
        email.execute(context=context)


def check_discrepancies_and_send_combined_email(
    interval_days, number_results, max_days, **kwargs
):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    start_date = (
        datetime.now() - timedelta(days=interval_days)).strftime('%Y-%m-%d')

    # OMS query
    query_oms = f'''
        SELECT s.*
        FROM PATAGONIA.CORE_TEST.OMS_SUBORDERS s
        LEFT JOIN PATAGONIA.CORE_TEST.ERP_PROCESSED_SALESLINE e
            ON s.ECOMMERCE_NAME = e.PURCHORDERFORMNUM
            AND e.SALESPOOLID LIKE 'ECOM'
        WHERE e.PURCHORDERFORMNUM IS NULL
            AND s.ORDER_DATE >= '{start_date}'
        ORDER BY s.ORDER_DATE ASC;
    '''
    cursor.execute(query_oms)
    columns_oms = [col[0] for col in cursor.description]
    data_oms = cursor.fetchall()
    df_oms = pd.DataFrame(data_oms, columns=columns_oms)
    df_oms.rename(
        columns={
            'ECOMMERCE_NAME': 'Número de orden',
            'ORDER_DATE': 'Fecha de creación',
            'ORDER_ID': 'ID de orden'
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
    df_oms_filtered['Fecha de creación'] = pd.to_datetime(
        df_oms_filtered['Fecha de creación']
    ).sort_values(ascending=False)  # Ordenar en orden decreciente
    oms_count = len(df_oms_filtered)

    # Shopify query
    query_shopify = f'''
        SELECT s.*
        FROM PATAGONIA.CORE_TEST.SHOPIFY_ORDERS s
        LEFT JOIN PATAGONIA.CORE_TEST.ERP_PROCESSED_SALESLINE e
            ON s.NAME = e.PURCHORDERFORMNUM
            AND e.SALESPOOLID LIKE 'ECOM'
        WHERE e.PURCHORDERFORMNUM IS NULL
            AND s.PROCESSED_AT > CURRENT_TIMESTAMP -
            INTERVAL '{interval_days} DAYS'
            AND s.FINANCIAL_STATUS = 'paid'
        ORDER BY s.NAME ASC;
    '''
    cursor.execute(query_shopify)
    columns_shopify = [col[0] for col in cursor.description]
    data_shopify = cursor.fetchall()
    df_shopify = pd.DataFrame(data_shopify, columns=columns_shopify)
    df_shopify.rename(
        columns={
            'NAME': 'Número de orden',
            'CREATED_AT': 'Fecha de creación',
            'ORDER_ID': 'ID de orden'
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
    df_shopify_filtered['Fecha de creación'] = pd.to_datetime(
        df_shopify_filtered['Fecha de creación']).sort_values(ascending=False)
    shopify_count = len(df_shopify_filtered)

    # Quantity discrepancy query
    query_quantity_discrepancy = f'''
    SELECT
    shop.ORDER_ID,
    shop.ORDER_NAME,
    shop.CREATED_AT,
    shop.total_cantidad_SHOPIFY,
    CAST(erp.total_cantidad_ERP AS INT) AS total_cantidad_ERP
FROM
    (SELECT
        s.ORDER_ID,
        s.ORDER_NAME,
        s.CREATED_AT,
        SUM(s.QUANTITY) AS total_cantidad_SHOPIFY
     FROM
        PATAGONIA.CORE_TEST.SHOPIFY_ORDERS_LINE s
     WHERE
        s.CREATED_AT >= DATEADD(DAY, -{interval_days}, CURRENT_DATE)
     GROUP BY
        s.ORDER_ID,
        s.ORDER_NAME,
        s.CREATED_AT) AS shop
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
    shop.total_cantidad_SHOPIFY != erp.total_cantidad_ERP
    AND NOT EXISTS (
        SELECT 1
        FROM PATAGONIA.CORE_TEST.ERP_PROCESSED_SALESLINE e
        WHERE e.PURCHORDERFORMNUM = CONCAT('NC-', shop.ORDER_NAME)
    )
    AND NOT EXISTS (
        SELECT 1
        FROM PATAGONIA.CORE_TEST.ERP_PROCESSED_SALESLINE e
        WHERE e.PURCHORDERFORMNUM = CONCAT(shop.ORDER_NAME, '1')
    )
ORDER BY
    TRY_TO_NUMBER(erp.PURCHORDERFORMNUM) ASC;
    '''
    cursor.execute(query_quantity_discrepancy)
    columns_quantity = [col[0] for col in cursor.description]
    data_quantity = cursor.fetchall()
    df_quantity = pd.DataFrame(data_quantity, columns=columns_quantity)
    df_quantity.rename(columns={
        'ORDER_NAME': 'Número de orden',
        'CREATED_AT': 'Fecha de creación'}, inplace=True)
    df_quantity['Número de orden'] = df_quantity.apply(
        lambda row: (
            f'<a href="https://admin.shopify.com/store/patagoniachile/orders/'
            f'{row["ORDER_ID"]}" target="_blank">'
            f'{row["Número de orden"]}</a>'
        ),
        axis=1
    )
    print(df_quantity.columns.tolist())
    df_quantity = df_quantity.sort_values(
        by='Número de orden', ascending=False)  # Ordenar en orden decreciente
    quantity_count = len(df_quantity)

    print(df_quantity)
    cursor.close()
    conn.close()

    # Alert conditions based on max_days and number_results
    now = pd.Timestamp.now()
    oms_older_than_max_days = df_oms_filtered['Fecha de creación'].apply(
        lambda x: (now - x).days > max_days
    ).any()
    discrep_older_than_max_days = df_quantity['Fecha de creación'].apply(
        lambda x: (now - x).days > max_days
    ).any()
    shopify_older_than_max_days = (
        df_shopify_filtered['Fecha de creación'].apply(
            lambda x: (now - x).days > max_days
        ).any())
    results_exceed_threshold = (
        len(df_oms_filtered) >= number_results or
        len(df_shopify_filtered) >= number_results or
        len(df_quantity) >= number_results
    )

    if (oms_older_than_max_days or discrep_older_than_max_days or
            shopify_older_than_max_days
            or results_exceed_threshold):
        # Convert DataFrames to HTML with count summaries
        df_oms_html = (
            f'<p>Se encontraron {oms_count} resultados:</p>' +
            df_oms_filtered.to_html(index=False, escape=False)
            if not df_oms_filtered.empty else (
                '<p>No hay discrepancias en OMS.</p>')
        )
        df_shopify_html = (
            f'<p>Se encontraron {shopify_count} resultados:</p>' +
            df_shopify_filtered.to_html(index=False, escape=False)
            if not df_shopify_filtered.empty else (
                '<p>No hay discrepancias en Shopify.</p>')
        )
        df_quantity_html = (
            f'<p>Se encontraron {quantity_count} resultados:</p>' +
            df_quantity.to_html(index=False, escape=False)
            if not df_quantity.empty else (
                '<p>No hay discrepancias entre Shopify y ERP.</p>'
            )
        )
        # Send alert email
        email_content = (
            '<p>Hay órdenes con discrepancias en OMS, Shopify o ERP:</p>'
            '<h3>Está en OMS pero no en ERP:</h3>'
            f'{df_oms_html}'
            '<h3>Está en Shopify pero no en ERP:</h3>'
            f'{df_shopify_html}'
            '<h3>Discrepancias en las cantidades entre OMS y Shopify:</h3>'
            f'{df_quantity_html}'
        )
        # Get email recipients from Airflow Variable or use default list
        email_recipients_str = Variable.get(
            'combined_discrepancies_emails',
            default_var=','.join(DEFAULT_EMAILS)
        )

        # Split email string by comma to get list of emails
        email_recipients = \
            [email.strip() for email in email_recipients_str.split(',')]

        # Send email using configured method (sendgrid or gmail)
        send_email(
            to_emails=email_recipients,
            subject='ALERTA: Discrepancias encontradas en Shopify, ERP y OMS',
            html_content=email_content,
            dag=kwargs['dag'],
            context=kwargs
        )
    else:
        print(
            'No significant discrepancies found in OMS, Shopify, or ERP.')


dag = DAG(
    'combined_oms_erp_shopify_discrepancies',
    default_args=default_args,
    description=(
        'DAG que revisa discrepancias entre OMS, Shopify y ERP'),
    schedule_interval='0 10 * * *',
    catchup=False,
    tags=['erp', 'oms', 'shopify', 'orders']
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
