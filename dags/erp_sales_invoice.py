from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from config.erp_sales_invoice_config import default_args
from utils.utils import write_data_to_snowflake
import os
import requests
import pandas as pd
from dotenv import load_dotenv
import logging
from urllib.parse import quote

# ============================================================================
# CONFIGURACIÓN PRINCIPAL
# ============================================================================

# Días hacia atrás para extraer facturas (por defecto)
# Puede ser sobreescrito por Variable de Airflow: 'erp_sales_invoice_days'
DAYS = 2
DAYS = Variable.get("erp_sales_invoice_days", default_var=DAYS)

# Tamaño de lote para escritura a Snowflake
# Cada cuántos registros escribir a Snowflake durante la extracción
BATCH_SIZE = 100000
BATCH_SIZE = Variable.get(
    "erp_sales_invoice_batch_size", default_var=BATCH_SIZE)

# Timeout extendido para requests OData (en segundos)
# Considerando que 10k registros toman ~20 segundos
REQUEST_TIMEOUT = 180
REQUEST_TIMEOUT = Variable.get(
    "erp_sales_invoice_request_timeout", default_var=REQUEST_TIMEOUT)

# ============================================================================

# Load environment variables from .env
load_dotenv()

ERP_URL = os.getenv('ERP_URL')
ERP_TOKEN_URL = os.getenv('ERP_TOKEN_URL')
ERP_CLIENT_ID = os.getenv('ERP_CLIENT_ID')
ERP_CLIENT_SECRET = os.getenv('ERP_CLIENT_SECRET')


def get_erp_token():
    """
    Obtains an access token for ERP API authentication.

    Returns:
        str: Access token for ERP API calls

    Raises:
        Exception: If authentication fails
    """
    # Validate that all required variables are defined
    if not all([ERP_URL, ERP_TOKEN_URL, ERP_CLIENT_ID, ERP_CLIENT_SECRET]):
        raise ValueError(
            "Missing required environment variables to connect to the ERP.")

    token_url = f'{ERP_TOKEN_URL}/oauth2/v2.0/token'
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': ERP_CLIENT_ID,
        'client_secret': ERP_CLIENT_SECRET,
        'scope': f'{ERP_URL}/.default'
    }
    token_headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    try:
        response = requests.post(
            token_url,
            data=token_data,
            headers=token_headers,
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            access_token = response.json().get('access_token')
            logging.info("Successfully obtained ERP access token")
            return access_token
        else:
            raise Exception(
                f"Error obtaining token: {response.status_code}"
                f" - {response.text}")
    except Exception as e:
        logging.error(f"Error during authentication: {e}")
        raise


def get_date_filter(days_back=None):
    """
    Creates a date filter for the OData query based on days back.

    Args:
        days_back (int): Number of days to go back from current date.
                        If None, uses Variable or DAYS constant.

    Returns:
        str: Formatted date filter for OData query
    """
    if days_back is None:
        # Try to get from Airflow Variable, fallback to DAYS constant
        try:
            days_back = int(Variable.get(
                "erp_sales_invoice_days",
                default_var=DAYS
            ))
        except (ValueError, TypeError):
            days_back = DAYS

    # Calculate the date threshold
    threshold_date = datetime.utcnow() - timedelta(days=days_back)
    formatted_date = threshold_date.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Create OData filter
    date_filter = f"InvoiceDate ge {formatted_date}"
    logging.info(f"Using date filter: {date_filter} (last {days_back} days)")

    return date_filter


def fetch_sales_invoices_page(token, skip=0, top=None, date_filter=None):
    """
    Fetches a single page of sales invoices from ERP OData endpoint.

    Args:
        token (str): ERP access token
        skip (int): Number of records to skip (for pagination)
        top (int): Number of records to return per page
        date_filter (str): OData date filter

    Returns:
        dict: Response containing sales invoices data and next link
    """
    if top is None:
        top = default_args['page_size']

    # Base URL for Sales Invoice Journal Headers
    base_url = f'{ERP_URL}/data/SalesInvoiceJournalHeaders'

    # Build query parameters
    params = {
        '$orderby': 'InvoiceDate desc',
        '$top': top,
        '$skip': skip
    }

    if date_filter:
        params['$filter'] = date_filter

    # Create the full URL
    query_string = '&'.join([f"{k}={quote(str(v))}"
                            for k, v in params.items()])
    url = base_url + '?' + query_string

    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    try:
        logging.info(f"Fetching sales invoices: skip={skip}, top={top}")
        response = requests.get(
            url,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                f"Error fetching sales invoices: {response.status_code}"
                f" - {response.text}")
    except Exception as e:
        logging.error(f"Error in API request: {e}")
        raise


def fetch_all_sales_invoices(token, days_back=None):
    """
    Fetches all sales invoices from ERP using pagination and writes to
    Snowflake in batches.

    Args:
        token (str): ERP access token
        days_back (int): Number of days to go back from current date

    Returns:
        dict: Summary of extraction process with total records processed
    """
    all_invoices = []
    skip = 0
    total_fetched = 0
    total_written = 0
    batch_count = 0

    # Get date filter
    date_filter = get_date_filter(days_back)

    logging.info("Starting to fetch sales invoices from ERP")
    logging.info(f"Using batch size: {BATCH_SIZE} records per Snowflake write")

    while True:
        try:
            # Fetch current page
            response_data = fetch_sales_invoices_page(
                token,
                skip=skip,
                date_filter=date_filter
            )

            # Extract invoices from current page
            invoices = response_data.get('value', [])

            if not invoices:
                logging.info("No more invoices to fetch")
                break

            all_invoices.extend(invoices)
            total_fetched += len(invoices)

            logging.info(f"Fetched {len(invoices)} invoices. "
                         f"Total so far: {total_fetched}")

            # Check if we should write a batch to Snowflake
            if len(all_invoices) >= BATCH_SIZE:
                batch_count += 1
                written_count = write_batch_to_snowflake(
                    all_invoices, batch_count
                )
                total_written += written_count

                logging.info(f"Batch {batch_count}: Wrote {written_count} "
                             f"records to Snowflake. "
                             f"Total written: {total_written}")

                # Clear the batch after writing
                all_invoices = []

            # Check if there's a next page
            next_link = response_data.get('@odata.nextLink')
            if not next_link:
                logging.info("No more pages available")
                break

            # Increment skip for next page
            skip += default_args['page_size']

            # Safety check to prevent infinite loops
            if total_fetched > 1000000:  # 1 million records limit
                logging.warning("Reached safety limit of 1M records")
                break

        except Exception as e:
            logging.error(f"Error fetching page at skip={skip}: {e}")
            raise

    # Write any remaining records in the final batch
    if all_invoices:
        batch_count += 1
        written_count = write_batch_to_snowflake(
            all_invoices, batch_count, is_final_batch=True
        )
        total_written += written_count

        logging.info(f"Final batch {batch_count}: Wrote {written_count} "
                     f"records to Snowflake. Total written: {total_written}")

    logging.info(f"Successfully fetched {total_fetched} total invoices")
    logging.info(f"Successfully wrote {total_written} total invoices "
                 f"to Snowflake")

    return {
        'total_fetched': total_fetched,
        'total_written': total_written,
        'batch_count': batch_count
    }


def write_batch_to_snowflake(invoices_list, batch_number,
                             is_final_batch=False):
    """
    Writes a batch of invoices to Snowflake.

    Args:
        invoices_list (list): List of invoice dictionaries
        batch_number (int): Current batch number for logging
        is_final_batch (bool): Whether this is the final batch

    Returns:
        int: Number of records written
    """
    if not invoices_list:
        logging.warning(f"Batch {batch_number}: No invoices to write")
        return 0

    try:
        logging.info(f"Batch {batch_number}: Preparing {len(invoices_list)} "
                     f"invoices for Snowflake")

        # Convert to DataFrame
        df = pd.DataFrame(invoices_list)

        # Clean and format data
        df = clean_sales_invoice_data(df)

        if df.empty:
            logging.warning(f"Batch {batch_number}: No data after cleaning")
            return 0

        # Write data to Snowflake
        write_data_to_snowflake(
            df,
            'ERP_SALES_INVOICE',
            default_args['erp_sales_invoice_table_columns'],
            ['INVOICENUMBER'],  # Primary key
            f'TEMP_ERP_SALES_INVOICE_BATCH_{batch_number}',
            default_args['snowflake_conn_id']
        )

        logging.info(f"Batch {batch_number}: Successfully wrote "
                     f"{len(df)} records to Snowflake")

        return len(df)

    except Exception as e:
        logging.error(f"Batch {batch_number}: Error writing to Snowflake: {e}")
        raise


def clean_sales_invoice_data(df):
    """
    Cleans and formats the sales invoice data.

    Args:
        df (pd.DataFrame): Raw sales invoice data

    Returns:
        pd.DataFrame: Cleaned and formatted data
    """
    if df.empty:
        return df

    logging.info("Cleaning and formatting sales invoice data")

    # Convert column names to uppercase to match Snowflake schema
    df.columns = df.columns.str.upper()

    # Handle date columns
    if 'INVOICEDATE' in df.columns:
        df['INVOICEDATE'] = pd.to_datetime(df['INVOICEDATE'], errors='coerce')
        # Convert to string format for Snowflake
        df['INVOICEDATE'] = df['INVOICEDATE'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Handle numeric columns
    numeric_columns = [
        'TOTALTAXAMOUNT', 'TOTALDISCOUNTCUSTOMERGROUPCODE',
        'TOTALCHARGEAMOUNT', 'TOTALDISCOUNTAMOUNT', 'TOTALINVOICEAMOUNT'
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Fill null string columns with empty strings
    string_columns = [
        'DATAAREAID', 'INVOICENUMBER', 'LEDGERVOUCHER',
        'INVOICEADDRESSCOUNTRYREGIONISOCODE', 'INVOICEADDRESSZIPCODE',
        'INVOICEADDRESSSTREETNUMBER', 'INVOICEADDRESSSTREET',
        'CURRENCYCODE', 'SALESORDERNUMBER', 'DELIVERYTERMSCODE',
        'CONTACTPERSONID', 'SALESORDERRESPONSIBLEPERSONNELNUMBER',
        'PAYMENTTERMSNAME', 'DELIVERYMODECODE', 'INVOICECUSTOMERACCOUNTNUMBER',
        'INVOICEADDRESSCITY', 'INVOICEADDRESSSTATE',
        'INVOICEADDRESSCOUNTRYREGIONID', 'CUSTOMERSORDERREFERENCE',
        'SALESORDERORIGINCODE'
    ]

    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].fillna('')

    # Remove the @odata.etag column if it exists
    if '@ODATA.ETAG' in df.columns:
        df = df.drop(columns=['@ODATA.ETAG'])

    # Ensure all expected columns exist
    expected_columns = [
        col[0] for col in default_args['erp_sales_invoice_table_columns']
        if col[0] not in ['SNOWFLAKE_CREATED_AT', 'SNOWFLAKE_UPDATED_AT']
    ]

    for col in expected_columns:
        if col not in df.columns:
            df[col] = ''  # Add missing columns with empty values

    # Select only the columns we need
    df = df[expected_columns]
    df.drop_duplicates(inplace=True)

    logging.info(f"Cleaned data shape: {df.shape}")
    logging.info(f"Date range: {df['INVOICEDATE'].min()} to "
                 f"{df['INVOICEDATE'].max()}")

    return df


def run_sales_invoice_extraction(**context):
    """
    Main function to execute sales invoice extraction from ERP to Snowflake.

    This function:
    1. Gets ERP access token
    2. Fetches sales invoices with pagination
    3. Writes data to Snowflake in batches
    """
    try:
        logging.info("Starting sales invoice extraction process")
        logging.info(f"Configuration: DAYS={DAYS}, BATCH_SIZE={BATCH_SIZE}, "
                     f"REQUEST_TIMEOUT={REQUEST_TIMEOUT}")

        # Get ERP token
        token = get_erp_token()

        # Fetch all sales invoices and write in batches
        extraction_summary = fetch_all_sales_invoices(token)

        if extraction_summary['total_fetched'] == 0:
            logging.warning("No sales invoices to process")
            return

        logging.info("Extraction completed successfully:")
        logging.info(f"  - Total invoices fetched: "
                     f"{extraction_summary['total_fetched']}")
        logging.info(f"  - Total invoices written: "
                     f"{extraction_summary['total_written']}")
        logging.info(f"  - Number of batches: "
                     f"{extraction_summary['batch_count']}")

        # Log any discrepancy between fetched and written
        if (extraction_summary['total_fetched'] !=
                extraction_summary['total_written']):
            logging.warning(f"Discrepancy detected: "
                            f"{extraction_summary['total_fetched']} "
                            f"fetched vs "
                            f"{extraction_summary['total_written']} written")

    except Exception as e:
        logging.error(f"Error in sales invoice extraction: {e}")
        raise


# DAG configuration
with DAG(
    'erp_sales_invoice_data',
    default_args=default_args,
    description='DAG to extract Sales Invoice data from ERP and '
                'load into Snowflake',
    schedule_interval='0 11 * * *',  # Daily at 11:00 AM
    catchup=False,
    tags=['erp', 'orders', 'invoice']
) as dag:

    extract_sales_invoices_task = PythonOperator(
        task_id='extract_sales_invoices',
        python_callable=run_sales_invoice_extraction,
        doc_md="""
        ### Extract Sales Invoices from ERP

        This task:
        - Authenticates with ERP using OAuth2
        - Fetches sales invoices using OData endpoint with enhanced timeout
        - Handles pagination automatically
        - Filters by date (configurable via Airflow Variables)
        - Writes data to Snowflake in configurable batches
        - Processes large datasets efficiently

        **Configuration Constants:**
        - `DAYS = 30`: Default days to go back (overrideable by Variable)
        - `BATCH_SIZE = 100000`: Records per Snowflake write batch
        - `REQUEST_TIMEOUT = 180`: Extended timeout for OData requests

        **Airflow Variables:**
        - `erp_sales_invoice_days`: Override DAYS constant

        **Performance:**
        - Handles millions of records with batch processing
        - Extended timeout for large OData responses (~20s per 10k records)
        - Memory efficient with streaming write approach

        **Dependencies:**
        - ERP_SALES_INVOICE table must exist in Snowflake
        - Environment variables for ERP connection must be configured
        """
    )
