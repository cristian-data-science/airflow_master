from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from dotenv import load_dotenv
from config.billing_data_config import default_args
from utils.utils import write_data_to_snowflake
import os
import pandas as pd
import requests
import re
from html import unescape
from lxml import etree
from datetime import datetime, timedelta
import pytz

load_dotenv()
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')

# SOAP service configuration from environment variables
BLUELINE_URL = os.getenv('BLUELINE_URL')
BLUELINE_RUT_EMISOR = os.getenv('BLUELINE_RUT_EMISOR')
BLUELINE_DV_EMISOR = os.getenv('BLUELINE_DV_EMISOR', '0')

# Use Airflow variables for configuration
USE_TODAY = Variable.get(
    "blueline_use_today", default_var="true").lower() == "true"

if USE_TODAY:
    local_tz = pytz.timezone('America/Santiago')
    now = datetime.now(local_tz)
    yesterday = now - timedelta(days=2)
    START_DATE = yesterday.strftime('%Y-%m-%d')
    END_DATE = now.strftime('%Y-%m-%d')
else:
    START_DATE = Variable.get(
        "blueline_start_date",
        default_var=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    )
    END_DATE = Variable.get(
        "blueline_end_date",
        default_var=datetime.now().strftime('%Y-%m-%d')
    )

# Document types to process
DTE_TYPES = Variable.get(
    "blueline_dte_types", default_var="33,39,61,52"
).split(',')

# DAG definition
dag = DAG(
    'blueline_financial_documents_data',
    default_args=default_args,
    description=('DAG to extract financial documents information from'
                 ' SOAP service and load it into Snowflake'),
    schedule_interval='15 11 * * *',
    catchup=False,
)

# Regex pattern to fix ampersands that are not part of an entity
_amp_fix = re.compile(r'&(?!#?\w+;)')


def consumir_servicio(rut_emisor, dv_emisor, tipo_dte, f_ini, f_fin):
    """
    Calls the SOAP service to get financial documents information.

    Args:
        rut_emisor: RUT of the issuer
        dv_emisor: Verification digit of the issuer
        tipo_dte: Type of DTE document
        f_ini: Start date in format YYYY-MM-DD
        f_fin: End date in format YYYY-MM-DD

    Returns:
        str: XML SOAP response
    """
    url = BLUELINE_URL

    payload = f"""<?xml version="1.0" encoding="UTF-8"?>
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                      xmlns:bl="http://bl">
      <soapenv:Header/>
      <soapenv:Body>
        <bl:consultaDTEEmitidos>
          <rutEmisor>{rut_emisor}</rutEmisor>
          <dvEmisor>{dv_emisor}</dvEmisor>
          <tipoDTE>{tipo_dte}</tipoDTE>
          <fechaInicio>{f_ini}</fechaInicio>
          <fechaFin>{f_fin}</fechaFin>
        </bl:consultaDTEEmitidos>
      </soapenv:Body>
    </soapenv:Envelope>"""

    resp = requests.post(
        url,
        headers={"Content-Type": "text/xml", "SOAPAction": ""},
        data=payload.encode("utf-8"),
        timeout=60,
    )
    resp.raise_for_status()
    return resp.text


def parsear_respuesta_soap(xml_soap):
    """
    Parses the SOAP response and extracts the billing information.

    Args:
        xml_soap: XML SOAP response

    Returns:
        list: List of dictionaries with billing information
    """
    ns = {
        "soap": "http://schemas.xmlsoap.org/soap/envelope/",
        "bl": "http://bl",
    }

    # Parse the SOAP envelope
    root = etree.fromstring(xml_soap.encode("utf-8"))

    ret = root.find(".//bl:consultaDTEEmitidosReturn", namespaces=ns)
    if ret is None or not ret.text:
        raise ValueError("Nodo <consultaDTEEmitidosReturn> vacío o ausente")

    # Unescape HTML entities
    interno = unescape(ret.text)

    # Fix loose ampersands
    interno = _amp_fix.sub("&amp;", interno)

    # Parse internal XML
    interno_root = etree.fromstring(
        interno.encode("utf-8"),
        parser=etree.XMLParser(recover=True)
    )

    # Extract DTEs
    dtes = []
    for dte in interno_root.findall("DTE"):
        tipo_dte = dte.findtext("TipoDTE")
        folio = dte.findtext("Folio")
        fecha_emis = dte.findtext("FechEmis")

        # Convert date format from DD/MM/YYYY to YYYY-MM-DD
        if fecha_emis:
            try:
                fecha_dt = datetime.strptime(fecha_emis, "%d/%m/%Y")
                fecha_emis = fecha_dt.strftime("%Y-%m-%d")
            except ValueError:
                # Keep original format if conversion fails
                pass

        # Create unique ID as TipoDTE-Folio
        id_dte = f"{tipo_dte}-{folio}" if tipo_dte and folio else ""

        # Handle 'null' values for numeric fields
        neto = dte.findtext("Neto")
        neto = None if neto == "null" else neto

        exento = dte.findtext("Exento")
        exento = None if exento == "null" else exento

        iva = dte.findtext("IVA")
        iva = None if iva == "null" else iva

        total = dte.findtext("Total")
        total = None if total == "null" else total

        dtes.append({
            "ID": id_dte,
            "TIPO_DTE": tipo_dte,
            "FOLIO": folio,
            "FECHA_EMISION": fecha_emis,
            "RUT_RECEPTOR": dte.findtext("RUTReceptor"),
            "RAZON_SOCIAL_RECEPTOR": dte.findtext("RazonSocialReceptor"),
            "NETO": neto,
            "EXENTO": exento,
            "IVA": iva,
            "TOTAL": total,
            "ESTADO_SII": dte.findtext("EstadoSII"),
            "URL": dte.findtext("URL"),
        })
    return dtes


def get_billing_data(tipo_dte, start_date, end_date):
    """
    Fetches billing data from the SOAP service for the specified document
    type and date range.

    Args:
        tipo_dte: Type of DTE document to fetch
        start_date: Start date in format YYYY-MM-DD
        end_date: End date in format YYYY-MM-DD

    Returns:
        pd.DataFrame: DataFrame with billing data
    """
    print(
        f'[Start execution] Fetching billing data for DTE type {tipo_dte} '
        f'from {start_date} to {end_date}'
    )

    try:
        # Call SOAP service for the specified date range and document type
        xml_soap = consumir_servicio(
            rut_emisor=BLUELINE_RUT_EMISOR,
            dv_emisor=BLUELINE_DV_EMISOR,
            tipo_dte=tipo_dte,
            f_ini=start_date,
            f_fin=end_date,
        )

        # Parse SOAP response
        documentos = parsear_respuesta_soap(xml_soap)
        print(f"Se obtuvieron {len(documentos)} DTEs")

        if not documentos:
            return pd.DataFrame()

        # Convert to DataFrame
        df_billing = pd.DataFrame(documentos)

        # Convert numeric columns
        numeric_cols = ['NETO', 'EXENTO', 'IVA', 'TOTAL']
        for col in numeric_cols:
            df_billing[col] = pd.to_numeric(df_billing[col], errors='coerce')

        # Convert date column
        df_billing['FECHA_EMISION'] = \
            pd.to_datetime(
                df_billing['FECHA_EMISION']
                ).dt.strftime('%Y-%m-%d')

        print(df_billing.head())
        print(df_billing.shape)
        return df_billing

    except Exception as e:
        print(f"ERROR: {e}")
        return pd.DataFrame()


def process_date_range(start_date, end_date):
    """
    Generates a list of date pairs (start_date, end_date) for each day
    in the range.
    Each pair represents a single day.

    Args:
        start_date: Start date in format YYYY-MM-DD
        end_date: End date in format YYYY-MM-DD

    Returns:
        list: List of tuples with (day_start, day_end) for each day
    """
    date_pairs = []
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')

    while current_date < end_date_obj:
        day_start = current_date.strftime('%Y-%m-%d')
        day_end = (current_date + timedelta(days=1)).strftime('%Y-%m-%d')
        date_pairs.append((day_start, day_end))
        current_date += timedelta(days=1)

    return date_pairs


def run_get_billing_data(**context):
    """
    Executes the process to fetch billing data and load it into Snowflake.
    Iterates through all document types and date ranges.
    """
    print('='*80)
    print('STARTING BLUELINE BILLING DATA EXTRACTION')
    print('Configuration:')
    print(f'  - Document Types: {DTE_TYPES}')
    print(f'  - Date Range: {START_DATE} to {END_DATE}')
    print('='*80)

    # Process each date range as a separate day
    date_pairs = process_date_range(START_DATE, END_DATE)
    print(f'Processing {len(date_pairs)} day(s) in the date range')

    # Initialize an empty DataFrame to collect all results
    all_data = pd.DataFrame()

    # Process each document type and date range
    total_documents = 0
    for tipo_dte in DTE_TYPES:
        print('')  # Empty line for better readability
        print(f'Processing document type: {tipo_dte}')
        tipo_documents = 0

        for start_date, end_date in date_pairs:
            print(f'  - Date range: {start_date} to {end_date}')

            # Get data for this document type and date range
            df_billing = get_billing_data(
                tipo_dte.strip(), start_date, end_date)

            # Append to the combined DataFrame if not empty
            if not df_billing.empty:
                records_count = len(df_billing)
                print(f'    Found {records_count} records')
                total_documents += records_count
                tipo_documents += records_count

                all_data = pd.concat(
                    [all_data, df_billing], ignore_index=True
                )
            else:
                print('    No records found')

        print(f'  Total documents for type {tipo_dte}: {tipo_documents}')

    print('')  # Empty line for better readability
    print('='*80)
    print('EXTRACTION SUMMARY:')
    print(f'  - Total document types processed: {len(DTE_TYPES)}')
    print(f'  - Total days processed: {len(date_pairs)}')
    print(f'  - Total documents found: {total_documents}')

    # Write all collected data to Snowflake
    if not all_data.empty:
        all_data.drop_duplicates(inplace=True)
        print('')  # Empty line for better readability
        print(f'Writing {len(all_data)} records to Snowflake '
              'table BLUELINE_FINANCIAL_DOCUMENTS')
        write_data_to_snowflake(
            all_data,
            'BLUELINE_FINANCIAL_DOCUMENTS',
            default_args['billing_table_columns'],
            ['ID'],
            'TEMP_BLUELINE_FINANCIAL_DOCUMENTS',
            SNOWFLAKE_CONN_ID
        )
        print('Data successfully written to Snowflake')
    else:
        print('')  # Empty line for better readability
        print('No data found for the specified document types and date ranges')

    print('='*80)
    print('BLUELINE BILLING DATA EXTRACTION COMPLETED')
    print('='*80)


# Task
task_get_billing_data = PythonOperator(
    task_id='get_billing_data',
    python_callable=run_get_billing_data,
    dag=dag,
)

# No dependencies as there's only one task
