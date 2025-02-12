from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from config.erp_products_pricelist_data_config import default_args
import os
import requests
import pandas as pd
from utils.utils import write_data_to_snowflake

# Load environment variables from .env
load_dotenv()

# Environment variables for ERP connection
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')
ERP_URL = os.getenv('ERP_URL')
ERP_TOKEN_URL = os.getenv('ERP_TOKEN_URL')
ERP_CLIENT_ID = os.getenv('ERP_CLIENT_ID')
ERP_CLIENT_SECRET = os.getenv('ERP_CLIENT_SECRET')

BATCH_SIZE_FETCH = 10000
BATCH_SIZE_WRITE = 100000
MAX_RECORDS_LIMIT = 120000


def get_erp_token():
    """
    Retrieves the access token for the ERP (Dynamics) API.
    Raises an exception if any required environment variable is missing
    or if the token request fails.
    """
    if not all([ERP_URL, ERP_TOKEN_URL, ERP_CLIENT_ID, ERP_CLIENT_SECRET]):
        raise ValueError("Missing environment variables required to "
                         "connect to the ERP.")

    token_url = f"{ERP_TOKEN_URL}/oauth2/v2.0/token"
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': ERP_CLIENT_ID,
        'client_secret': ERP_CLIENT_SECRET,
        'scope': f"{ERP_URL}/.default"
    }
    token_headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    response = requests.post(token_url, data=token_data, headers=token_headers)
    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        raise Exception(f"Error obtaining token: {response.status_code} "
                        f"- {response.text}")


def fetch_sales_price_agreements_chunk(access_token, skip=0, top=10000):
    """
    Performs a GET request to the SalesPriceAgreements endpoint to retrieve
    a batch of records, using $skip and $top for pagination.

    Returns:
      (df, next_link):
        - df: DataFrame containing the records (could be empty if no data).
        - next_link: The next page URL (None if there's no next link).
    """
    endpoint = f"{ERP_URL}/data/SalesPriceAgreements?$skip={skip}&$top={top}"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    try:
        print("[INFO] Fetching chunk from "
              f"SalesPriceAgreements at skip={skip}")
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()
        data = response.json()

        # Convert the response "value" to a DataFrame
        records = data.get('value', [])
        df = pd.DataFrame(records)

        next_link = data.get('@odata.nextLink', None)
        return df, next_link

    except Exception as e:
        print(f"Error fetching chunk from SalesPriceAgreements: {e}")
        raise


def process_sales_price_agreements_df(df):
    """
    Performs data cleaning and type conversions for the DataFrame
    before loading into Snowflake.
    """
    if df.empty:
        return df

    # Convert date columns to datetime
    date_cols = ['PriceApplicableFromDate', 'PriceApplicableToDate']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Convert numeric columns
    numeric_cols = [
        'RecordId',
        'SalesPriceQuantity',
        'ToQuantity',
        'FixedPriceCharges',
        'SalesLeadTimeDays',
        'FromQuantity',
        'Price'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Fill NaN in text columns
    df = df.fillna('')

    return df


def fetch_and_load_pricelist_data():
    """
    Main function to:
      1. Obtain an access token.
      2. Paginate the SalesPriceAgreements endpoint in batches of
      `BATCH_SIZE_FETCH`.
      3. Accumulate records in memory. Once `batch_size_write` is
      reached (or exceeded),
         write to Snowflake using `write_data_to_snowflake`.
      4. If `max_records_limit` is set (not None), stop once that
      many total records
         have been processed.
    """
    print("[START] fetch_and_load_pricelist_data")

    access_token = get_erp_token()
    print("[INFO] Successfully retrieved token.")

    # We'll assume these columns are the primary key when merging
    primary_keys = [
        'ItemNumber',
        'ProductColorId',
        'ProductSizeId',
        'ProductStyleId',
        'ProductconfigurationId',
        'PriceCustomerGroupCode'
    ]

    skip = 0
    total_processed = 0

    # Accumulated data
    accumulated_dfs = []
    accumulated_count = 0

    while True:
        df_chunk, next_link = fetch_sales_price_agreements_chunk(
            access_token, skip=skip, top=BATCH_SIZE_FETCH
        )
        if df_chunk is None or df_chunk.empty:
            print(f"[INFO] No records at skip={skip}. Pagination ends.")
            break

        # If there's a limit, check if adding this chunk would exceed it
        chunk_size = len(df_chunk)
        if MAX_RECORDS_LIMIT is not None:
            if total_processed >= MAX_RECORDS_LIMIT:
                # We've already processed the limit
                print("[INFO] max_records_limit reached. Stopping.")
                break
            elif total_processed + chunk_size > MAX_RECORDS_LIMIT:
                # Partial chunk scenario
                allowed_size = MAX_RECORDS_LIMIT - total_processed
                df_chunk = df_chunk.iloc[:allowed_size]
                chunk_size = len(df_chunk)
                print(f"[INFO] Taking only {chunk_size} rows from this chunk "
                      f"due to MAX_RECORDS_LIMIT.")
                # After slicing, we won't fetch more pages
                next_link = None  # We can forcibly end pagination

        # Process chunk
        df_chunk = process_sales_price_agreements_df(df_chunk)
        chunk_size = len(df_chunk)  # re-check length after processing
        if chunk_size == 0:
            # If it's empty after a partial slice or processing
            print("[INFO] No valid rows left after partial slice or cleaning.")
            break

        # Accumulate in memory
        accumulated_dfs.append(df_chunk)
        accumulated_count += chunk_size
        total_processed += chunk_size
        skip += BATCH_SIZE_FETCH

        # Check if we reached or exceeded the write threshold
        if accumulated_count >= BATCH_SIZE_WRITE:
            df_to_write = pd.concat(accumulated_dfs, ignore_index=True)
            print(f"[INFO] Writing {len(df_to_write)} accumulated "
                  "records to Snowflake.")
            write_data_to_snowflake(
                df_to_write,
                'ERP_PRODUCTS_PRICELIST',
                default_args['snowflake_erp_pricelist_table_columns'],
                primary_keys,
                'TEMP_ERP_PRODUCTS_PRICELIST',
                SNOWFLAKE_CONN_ID
            )
            # Reset accumulators
            accumulated_dfs = []
            accumulated_count = 0

        # Check pagination stopping condition
        if not next_link:
            print("[INFO] No nextLink found or partial chunk used. "
                  "Pagination completed.")
            break

    # After exiting the loop, write any leftover records
    if accumulated_dfs:
        df_remaining = pd.concat(accumulated_dfs, ignore_index=True)
        if not df_remaining.empty:
            print(f"[INFO] Writing remaining {len(df_remaining)} "
                  "records to Snowflake.")
            write_data_to_snowflake(
                df_remaining,
                'ERP_PRODUCTS_PRICELIST',
                default_args['snowflake_erp_pricelist_table_columns'],
                primary_keys,
                'TEMP_ERP_PRODUCTS_PRICELIST',
                SNOWFLAKE_CONN_ID
            )

    print("[END] Data upserted into the final Snowflake table.")


def run_fetch_and_load_pricelist_data():
    """
    A wrapper function for Airflow, calling the main logic.
    """
    fetch_and_load_pricelist_data()


# DAG definition
dag = DAG(
    'erp_products_pricelist_data',
    default_args=default_args,
    description='DAG to extract product pricelist '
    'data (SalesPriceAgreements) from the ERP and upsert into Snowflake',
    schedule_interval='@weekly',
    catchup=False
)

fetch_and_load_task = PythonOperator(
    task_id='fetch_and_load_pricelist_data_task',
    python_callable=run_fetch_and_load_pricelist_data,
    dag=dag
)
