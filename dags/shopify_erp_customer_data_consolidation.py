from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector import connect
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin, urlencode, urlparse, parse_qs
import os
import pyodbc
import requests
import pandas as pd


# Load environment variables from .env file
load_dotenv()
SHOPIFY_API_KEY = os.getenv('SHOPIFY_API_KEY')
SHOPIFY_API_PASSWORD = os.getenv('SHOPIFY_API_PASSWORD')
SHOPIFY_API_URL = \
    os.getenv('SHOPIFY_API_URL') + os.getenv('SHOPIFY_API_VERSION') + '/'


# Default variables
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 19),
    'email': ['enrique.urrutia@patagonia.com'],
    'phone': 0,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'snowflake_shopify_customer_table_columns': [
        ('SHOPIFY_ID', 'NUMBER'),
        ('EMAIL', 'VARCHAR(255)'),
        ('ACCEPTS_MARKETING', 'BOOLEAN'),
        ('ACCEPTS_MARKETING_UPDATED_AT', 'TIMESTAMP_NTZ'),
        ('ADMIN_GRAPHQL_API_ID', 'VARCHAR(255)'),
        ('CREATED_AT', 'TIMESTAMP_NTZ'),
        ('CURRENCY', 'VARCHAR(255)'),
        ('FIRST_NAME', 'VARCHAR(255)'),
        ('LAST_NAME', 'VARCHAR(255)'),
        ('LAST_ORDER_ID', 'NUMBER'),
        ('LAST_ORDER_NAME', 'VARCHAR(255)'),
        ('MARKETING_OPT_IN_LEVEL', 'VARCHAR(255)'),
        ('MULTIPASS_IDENTIFIER', 'VARCHAR(255)'),
        ('NOTE', 'VARCHAR(255)'),
        ('ORDERS_COUNT', 'NUMBER'),
        ('PHONE', 'VARCHAR(255)'),
        ('STATE', 'VARCHAR(255)'),
        ('TAGS', 'VARCHAR(255)'),
        ('TAX_EXEMPT', 'BOOLEAN'),
        ('TOTAL_SPENT', 'FLOAT'),
        ('UPDATED_AT', 'TIMESTAMP_NTZ'),
        ('VERIFIED_EMAIL', 'BOOLEAN')
    ]
}

# Dag definition
dag = DAG(
    'shopify_erp_customer_data_consolidation',
    default_args=default_args,
    description='DAG to extract customer data from Shopify and ERP and '
    'consolidate it in a single table in Snowflake',
    schedule_interval=timedelta(days=1),
)


# Tasks functions
def get_shopify_customers(batch_limit=250, response_limit=None, days=1):
    """
    Fetches customer data from Shopify API with pagination support and filters
    based on the last updated date.

    Parameters:
    - batch_limit (int): Max number of records per API call. Defaults to 250.
    - response_limit (int, optional): Max total records to fetch. If None,
      fetches all. Defaults to None.
    - days (int): Number of past days to fetch records from, based on last
      updated date. Defaults to 1.

    Returns:
    - customers (list): List of customer records from Shopify.

    Iterates over Shopify API response pages, fetching records in batches.
    Continues requests until all customers fetched, response_limit reached,
    or no more pages left.

    If `days` is set, fetches records updated within last `days`. For each API
    request, prints customer count and accessed URL for monitoring.

    Raises:
    - HTTPError: If API request fails.
    """
    next_page_info = None
    customers = []
    params = {"limit": batch_limit}
    if days:
        params["updated_at_min"] = \
            (datetime.now() - timedelta(days=days)).isoformat()
    url = urljoin(SHOPIFY_API_URL, "customers.json")

    while url:
        # Make a GET request to the Shopify API using the provided URL.
        # The request includes parameters for pagination and authentication.
        response = requests.get(
            url,
            params=params,
            auth=HTTPBasicAuth(SHOPIFY_API_KEY, SHOPIFY_API_PASSWORD)
        )

        # Raises an HTTPError if the request returned an error status code.
        response.raise_for_status()

        # Append the fetched customers to the customers list. The response is
        # expected to be in JSON format with a key "customers".
        customers.extend(response.json()["customers"])

        # If a response limit is set and the number of fetched customers
        # reaches or exceeds this limit, break out of the loop.
        if response_limit and len(customers) >= response_limit:
            break

        # Extracts the 'Link' header from the response headers. This header
        # contains URLs for pagination (next page, previous page).
        link_header = response.headers.get("Link")

        if link_header:
            # Split the Link header to extract individual links.
            links = link_header.split(", ")

            # Reset URL to ensure fresh assignment from the Link header.
            url = None

            for link in links:
                # Check for the presence of a 'next' relation type in the Link.
                if 'rel="next"' in link:
                    # Extract the URL for the next page of results.
                    url = link[link.index("<")+1:link.index(">")]

                    # Parse the query string from the URL to
                    # extract the 'page_info'.
                    query_string = urlparse(url).query
                    next_page_info = parse_qs(
                        query_string).get('page_info', [None])[0]

                    # Clear the params for the next request, as the 'next' URL
                    # already contains the required parameters.
                    params = None
                    break
        else:
            # If there is no 'Link' header in the response, set URL to None to
            # stop further pagination.
            url = None

    return customers


def customers_to_dataframe(customers_datalist):
    """
    Converts a list of customer data into a Pandas DataFrame.

    This function is useful for transforming data retrieved from an API
    into a structured format.

    Parameters:
    - customers_datalist (list): A list of dictionaries, each representing
        a customer's data.

    Returns:
    - DataFrame: A Pandas DataFrame containing the customer data,
        with appropriately named columns.

    The function first checks if `customers_datalist` is not empty.
    It then converts the list of dictionaries into a Pandas DataFrame,
    renames the columns to match the specified column names, and reorders
    the columns according to a predefined order. It prints the first few
    rows of the DataFrame for review and returns the processed DataFrame.
    """
    if customers_datalist is not None:
        df = pd.DataFrame(customers_datalist)
        df = df.rename(columns={
            'id': 'SHOPIFY_ID',
            'email': 'EMAIL',
            'accepts_marketing': 'ACCEPTS_MARKETING',
            'created_at': 'CREATED_AT',
            'updated_at': 'UPDATED_AT',
            'first_name': 'FIRST_NAME',
            'last_name': 'LAST_NAME',
            'orders_count': 'ORDERS_COUNT',
            'state': 'STATE',
            'total_spent': 'TOTAL_SPENT',
            'last_order_id': 'LAST_ORDER_ID',
            'note': 'NOTE',
            'verified_email': 'VERIFIED_EMAIL',
            'multipass_identifier': 'MULTIPASS_IDENTIFIER',
            'tax_exempt': 'TAX_EXEMPT',
            'tags': 'TAGS',
            'last_order_name': 'LAST_ORDER_NAME',
            'currency': 'CURRENCY',
            'phone': 'PHONE',
            'accepts_marketing_updated_at': 'ACCEPTS_MARKETING_UPDATED_AT',
            'marketing_opt_in_level': 'MARKETING_OPT_IN_LEVEL',
            'admin_graphql_api_id': 'ADMIN_GRAPHQL_API_ID',
        })
        df = df[['SHOPIFY_ID', 'EMAIL', 'ACCEPTS_MARKETING', 'CREATED_AT',
                 'UPDATED_AT', 'FIRST_NAME', 'LAST_NAME', 'ORDERS_COUNT',
                 'STATE', 'TOTAL_SPENT', 'LAST_ORDER_ID', 'NOTE',
                 'VERIFIED_EMAIL', 'MULTIPASS_IDENTIFIER', 'TAX_EXEMPT',
                 'TAGS', 'LAST_ORDER_NAME', 'CURRENCY', 'PHONE',
                 'ACCEPTS_MARKETING_UPDATED_AT', 'MARKETING_OPT_IN_LEVEL',
                 'ADMIN_GRAPHQL_API_ID']]
        print(df.head().to_string())
        return df
    else:
        print("No data received from get_shopify_customers")


def create_snowflake_temporary_table(cursor, temp_table_name):
    """
    Creates a temporary table in Snowflake with a defined column structure.

    This function is useful for preparing the Snowflake environment for data
    insertion or update operations.

    Parameters:
    - cursor: A database cursor for Snowflake to execute SQL commands.
    - temp_table_name (str): The name of the temporary table to be created.

    The function uses the provided cursor to execute an SQL command that
    creates a temporary table in Snowflake. The table structure is defined
    based on the columns specified in
    `snowflake_shopify_customer_table_columns` within `default_args`.
    """
    columns = default_args['snowflake_shopify_customer_table_columns']
    create_temp_table_sql = f"CREATE TEMPORARY TABLE {temp_table_name} ("
    create_temp_table_sql += \
        ", ".join([f"{name} {type}" for name, type in columns]) + ");"

    print(create_temp_table_sql)
    cursor.execute(create_temp_table_sql)


def write_data_to_snowflake(df, table_name):
    """
    Writes a Pandas DataFrame to a Snowflake table.

    Parameters:
    - df (pandas.DataFrame): DataFrame to be written to Snowflake.
    - table_name (str): Name of the target table in Snowflake.

    Utilizes the SnowflakeHook from Airflow to establish a connection.
    The write_pandas method from snowflake-connector-python is used to
    write the DataFrame.
    """
    # Use the SnowflakeHook to get a connection object
    hook = SnowflakeHook(snowflake_conn_id='patagonia_snowflake_connection')
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Create temporary table
    temporary_table_name = "TEMP_SHOPIFY_CUSTOMERS"

    try:
        # Create Temporary Table
        create_snowflake_temporary_table(cursor, temporary_table_name)

        # Write the DataFrame to Snowflake Temporary Table
        success, nchunks, nrows, _ = \
            write_pandas(conn, df, temporary_table_name)
        if not success:
            raise Exception(f"Failed to write to {table_name} in Snowflake.")
        print(f"Successfully wrote {nrows} rows to "
              f"{temporary_table_name} in Snowflake.")

        # Generate UPDATE y INSERT by snowflake_shopify_customer_table_columns
        update_set_parts = []
        insert_columns = []
        insert_values = []

        for column, _ in default_args[
                'snowflake_shopify_customer_table_columns']:
            update_set_parts.append(
                f"{table_name}.{column} = new_data.{column}")
            insert_columns.append(column)
            insert_values.append(f"new_data.{column}")

        update_set_sql = ",\n".join(update_set_parts)
        insert_columns_sql = ", ".join(insert_columns)
        insert_values_sql = ", ".join(insert_values)

        # Snowflake Merge execute
        cursor.execute('BEGIN')
        merge_sql = f"""
        MERGE INTO {table_name} USING {temporary_table_name} AS new_data
        ON {table_name}.SHOPIFY_ID = new_data.SHOPIFY_ID
        WHEN MATCHED THEN
            UPDATE SET
                {update_set_sql}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns_sql})
            VALUES ({insert_values_sql});
         """
        cursor.execute(merge_sql)

        duplicates = check_duplicates_sql(cursor, table_name)
        if duplicates:
            cursor.execute("ROLLBACK")
            print(f"There are duplicates: {duplicates}. ROLLBACK executed.")
        else:
            cursor.execute("COMMIT")
            print(f'Table {table_name} modified successfully!')
    except Exception as e:
        cursor.execute("ROLLBACK")
        raise e
    finally:
        cursor.close()


def run_get_shopify_customers():
    """
    A wrapper function that chains fetching customer data from Shopify,
    transforming it into a DataFrame, and subsequently writing it into
    Snowflake.

    This function calls `get_shopify_customers` to fetch customer data
    from Shopify, transforms the fetched data into a DataFrame using
    `customers_to_dataframe`, and writes the DataFrame to Snowflake
    using the `write_data_to_snowflake` function.
    """
    customers_datalist = \
        get_shopify_customers(batch_limit=200, response_limit=None, days=3)
    customers_dataframe = \
        customers_to_dataframe(customers_datalist)
    write_data_to_snowflake(customers_dataframe, 'SHOPIFY_CUSTOMERS')


def check_duplicates_sql(cursor, table_name):
    """
    Checks for duplicate records in a specified Snowflake table.

    This function executes an SQL query to identify duplicate entries
    based on the SHOPIFY_ID column.

    Parameters:
    - cursor: A database cursor to execute the query in Snowflake.
    - table_name (str): The name of the table to check for duplicates.

    Returns:
    - list: A list of tuples containing the SHOPIFY_IDs and the count
    of their occurrences, if duplicates are found.

    The function executes an SQL query that groups records by SHOPIFY_ID
    and counts occurrences, looking for counts greater than one.
    If duplicates are found, it returns the list of these records.
    In case of an exception, it performs a rollback and prints the error.
    """
    check_duplicates_sql = f"""
    SELECT SHOPIFY_ID, COUNT(*)
    FROM {table_name}
    GROUP BY SHOPIFY_ID
    HAVING COUNT(*) > 1;
    """
    try:
        cursor.execute(check_duplicates_sql)
        return cursor.fetchall()
    except Exception as e:
        cursor.execute("ROLLBACK")
        print("ROLLBACK executed due to an error:", e)


# Task definitions
task_1 = PythonOperator(
    task_id='get_shopify_customers',
    python_callable=run_get_shopify_customers,
    dag=dag,
)
