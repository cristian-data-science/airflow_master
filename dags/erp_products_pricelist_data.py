from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from config.erp_products_pricelist_data_config import default_args
import os
import requests
import json
import time
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

# Constants for batching logic
BATCH_SIZE_FETCH = 10000
BATCH_SIZE_WRITE = 100000
MAX_RECORDS_LIMIT = None
SKIP = 0


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


def fetch_sales_price_agreements_chunk(
        access_token, skip=0, top=10000, max_retries=3):
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

    attempt = 0
    while attempt < max_retries:
        try:
            print(f"[INFO] Fetching chunk from skip={skip} "
                  f"(attempt={attempt+1})")
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()

            # Try parsing JSON
            try:
                data = response.json()
                records = data.get('value', [])
                df = pd.DataFrame(records)
                next_link = data.get('@odata.nextLink', None)
                return df, next_link
            except json.JSONDecodeError as jde:
                print(f"[WARN] JSONDecodeError on attempt {attempt+1}: {jde}")
                # Optionally print partial response or do something else
                attempt += 1
                time.sleep(5)  # backoff
                continue  # retry

        except Exception as e:
            print(f"[ERROR] Error fetching chunk: {e}")
            attempt += 1
            time.sleep(5)

    # If we exhausted retries, raise an exception
    raise Exception(f"Failed to fetch chunk at skip={skip} "
                    f"after {max_retries} retries due to JSON errors.")


def process_sales_price_agreements_df(df):
    """
    Performs data cleaning, type conversions, and excludes unwanted columns
    before loading into Snowflake.
    """
    if df.empty:
        return df

    # Exclude columns we don't need
    cols_to_exclude = ['@odata.etag', 'dataAreaId']
    df.drop(columns=cols_to_exclude, inplace=True, errors='ignore')

    # Rename all columns to uppercase
    df.columns = [col.upper() for col in df.columns]

    # Convert date columns to datetime
    date_cols = ['PRICEAPPLICABLEFROMDATE', 'PRICEAPPLICABLETODATE']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Convert numeric columns
    numeric_cols = [
        'RECORDID',
        'SALESPRICEQUANTITY',
        'TOQUANTITY',
        'FIXEDPRICECHARGES',
        'SALESLEADTIMEDAYS',
        'FROMQUANTITY',
        'PRICE'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Fill NaN in text columns
    df.fillna('', inplace=True)

    return df


def fetch_and_load_pricelist_data():
    """
    Main function to:
      1. Obtain an access token.
      2. Paginate the SalesPriceAgreements endpoint in batches of
      `BATCH_SIZE_FETCH`.
      3. Accumulate records in memory. Once `BATCH_SIZE_WRITE` is
      reached (or exceeded),
         write to Snowflake using `write_data_to_snowflake`.
      4. If `MAX_RECORDS_LIMIT` is set (not None), stop once that
      many total records
         have been processed.
    """
    print("[START] fetch_and_load_pricelist_data")

    access_token = get_erp_token()
    print("[INFO] Successfully retrieved token.")

    # We'll assume these columns are the primary key when merging
    primary_keys = [
        'RECORDID'
    ]

    skip = SKIP
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

        chunk_size = len(df_chunk)

        # Check limit
        if MAX_RECORDS_LIMIT is not None:
            if total_processed >= MAX_RECORDS_LIMIT:
                print("[INFO] MAX_RECORDS_LIMIT reached. Stopping.")
                break
            elif total_processed + chunk_size > MAX_RECORDS_LIMIT:
                allowed_size = MAX_RECORDS_LIMIT - total_processed
                df_chunk = df_chunk.iloc[:allowed_size]
                chunk_size = len(df_chunk)
                print(f"[INFO] Taking only {chunk_size} rows from this chunk "
                      f"due to MAX_RECORDS_LIMIT.")
                next_link = None  # forcibly end pagination after partial chunk

        # Process chunk
        df_chunk = process_sales_price_agreements_df(df_chunk)
        print("[INFO] SalesPriceAgreements DataFrame to load.")
        print(f"[INFO] Initial shape: {df_chunk.shape}")
        print(df_chunk.head())

        chunk_size = len(df_chunk)  # recalc after processing
        if chunk_size == 0:
            print("[INFO] No valid rows after partial slice or cleaning.")
            break

        # Accumulate
        accumulated_dfs.append(df_chunk)
        accumulated_count += chunk_size
        total_processed += chunk_size
        skip += BATCH_SIZE_FETCH

        # Write if we reached/exceeded the batch size
        if accumulated_count >= BATCH_SIZE_WRITE:
            df_to_write = pd.concat(accumulated_dfs, ignore_index=True)
            print(f"[INFO] Writing {len(df_to_write)} accumulated records"
                  " to Snowflake.")
            write_data_to_snowflake(
                df_to_write,
                'ERP_PRODUCTS_PRICELIST',
                default_args['snowflake_erp_pricelist_table_columns'],
                primary_keys,
                'TEMP_ERP_PRODUCTS_PRICELIST',
                SNOWFLAKE_CONN_ID
            )
            accumulated_dfs = []
            accumulated_count = 0

        if not next_link:
            print("[INFO] No nextLink found or partial chunk used. "
                  "Pagination completed.")
            break

    # Write any leftovers
    if accumulated_dfs:
        df_remaining = pd.concat(accumulated_dfs, ignore_index=True)
        if not df_remaining.empty:
            print(f"[INFO] Writing remaining {len(df_remaining)} records "
                  "to Snowflake.")
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
