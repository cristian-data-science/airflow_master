from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from dags.config.erp_customer_data_config import default_args
import os
import pymssql
import pandas as pd


# Load environment variables from .env file
load_dotenv()
SHOPIFY_API_KEY = os.getenv('SHOPIFY_API_KEY')
SHOPIFY_API_PASSWORD = os.getenv('SHOPIFY_API_PASSWORD')
SHOPIFY_API_URL = \
    os.getenv('SHOPIFY_API_URL') + os.getenv('SHOPIFY_API_VERSION') + '/'

BYOD_SERVER = os.getenv('BYOD_SERVER')
BYOD_DATABASE = os.getenv('BYOD_DATABASE')
BYOD_USERNAME = os.getenv('BYOD_USERNAME')
BYOD_PASSWORD = os.getenv('BYOD_PASSWORD')

# Dag definition
dag = DAG(
    'erp_customer_data',
    default_args=default_args,
    description='DAG to extract customer data from ERP and '
    'consolidate it in a single table in Snowflake',
    schedule_interval=timedelta(days=1),
)


# Tasks functions
def get_byod_customers(days=0):
    conn = pymssql.connect(
        BYOD_SERVER, BYOD_USERNAME, BYOD_PASSWORD, BYOD_DATABASE
    )
    cursor = conn.cursor(as_dict=True)
    table_name = default_args['byod_erp_customer_table_name']
    columns = ', '.join([
        column[0]
        for column in default_args['byod_erp_customer_table_interest_columns']
    ])
    query = f'SELECT {columns} FROM {table_name}'
    if days is not None:
        query += f'''
        WHERE CAST(SYNCSTARTDATETIME AS DATE)
        BETWEEN
            CAST(DATEADD(DAY, -{days}, GETDATE()) AS DATE)
            AND CAST(GETDATE() AS DATE)'''
    cursor.execute(query)
    df_byod_customers = pd.DataFrame(cursor.fetchall())
    df_byod_customers['SYNCSTARTDATETIME'] = \
        df_byod_customers['SYNCSTARTDATETIME'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_byod_customers['ADDRESSVALIDFROM'] = \
        df_byod_customers['ADDRESSVALIDFROM'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_byod_customers['DELIVERYADDRESSVALIDFROM'] = \
        df_byod_customers[
            'DELIVERYADDRESSVALIDFROM'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_byod_customers['CREDMANCUSTOMERSINCE'] = \
        df_byod_customers[
            'CREDMANCUSTOMERSINCE'].dt.strftime('%Y-%m-%d %H:%M:%S')
    print(df_byod_customers.head())
    print(df_byod_customers.shape)
    return df_byod_customers


def create_snowflake_temporary_table(cursor, temp_table_name, columns):
    """
    Creates a temporary table in Snowflake with a defined column structure.

    This function is useful for preparing the Snowflake environment for data
    insertion or update operations.

    Parameters:
    - cursor: A database cursor for Snowflake to execute SQL commands.
    - temp_table_name (str): The name of the temporary table to be created.

    The function uses the provided cursor to execute an SQL command that
    creates a temporary table in Snowflake. The table structure is defined
    based on the columns specified in columns.
    """
    create_temp_table_sql = f"CREATE TEMPORARY TABLE {temp_table_name} ("
    create_temp_table_sql += \
        ", ".join([f"{name} {type}" for name, type in columns]) + ");"

    print(create_temp_table_sql)
    cursor.execute(create_temp_table_sql)


def write_data_to_snowflake(
        df, table_name, columns, primary_key, temporary_table_name
        ):
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
    hook = SnowflakeHook(snowflake_conn_id=default_args['snowflake_conn_id'])
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Create Temporary Table
        create_snowflake_temporary_table(
            cursor,
            temporary_table_name,
            columns
        )

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

        for column, _ in columns:
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
        ON {table_name}.{primary_key} = new_data.{primary_key}
        WHEN MATCHED THEN
            UPDATE SET
                {update_set_sql},
                snowflake_updated_at = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns_sql},
                    snowflake_created_at, snowflake_updated_at)
            VALUES ({insert_values_sql},
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
         """
        cursor.execute(merge_sql)

        duplicates = check_duplicates_sql(cursor, table_name, primary_key)
        if duplicates:
            cursor.execute("ROLLBACK")
            print(f"There are duplicates: {duplicates}. ROLLBACK executed.")
        else:
            cursor.execute("COMMIT")
            cursor.execute(f'''SELECT COUNT(*) FROM {table_name}
                           WHERE DATE(snowflake_created_at) = CURRENT_DATE''')
            new_rows = cursor.fetchone()
            cursor.execute(f'''SELECT COUNT(*) FROM {table_name}
                           WHERE DATE(snowflake_updated_at) = CURRENT_DATE
                            AND DATE(snowflake_created_at) <> CURRENT_DATE''')
            updated_rows = cursor.fetchone()

            print(f'''
            [EXECUTION RESUME]
            Table {table_name} modified successfully!
            - New inserted rows: {new_rows[0]}
            - Updated rows: {updated_rows[0]}
            ''')

    except Exception as e:
        cursor.execute("ROLLBACK")
        raise e
    finally:
        cursor.close()


def check_duplicates_sql(cursor, table_name, primary_key):
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
    SELECT {primary_key}, COUNT(*)
    FROM {table_name}
    GROUP BY {primary_key}
    HAVING COUNT(*) > 1;
    """
    try:
        cursor.execute(check_duplicates_sql)
        return cursor.fetchall()
    except Exception as e:
        cursor.execute("ROLLBACK")
        print("ROLLBACK executed due to an error:", e)


def run_get_byod_customers():
    df_byod_customers = get_byod_customers(days=2)
    write_data_to_snowflake(
        df_byod_customers,
        'ERP_CUSTOMERS',
        default_args['byod_erp_customer_table_interest_columns'],
        'CUSTOMERACCOUNT',
        'TEMP_ERP_CUSTOMERS'
    )


# Task definitions
task_1 = PythonOperator(
    task_id='get_byod_customers',
    python_callable=run_get_byod_customers,
    dag=dag,
)
