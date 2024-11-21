from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas


def create_snowflake_temporary_table(cursor, temp_table_name, columns):
    '''
    Creates a temporary table in Snowflake with a defined column structure.

    This function is useful for preparing the Snowflake environment for data
    insertion or update operations.

    Parameters:
    - cursor: A database cursor for Snowflake to execute SQL commands.
    - temp_table_name (str): The name of the temporary table to be created.

    The function uses the provided cursor to execute an SQL command that
    creates a temporary table in Snowflake. The table structure is defined
    based on the columns specified in columns.
    '''
    create_temp_table_sql = f'CREATE TEMPORARY TABLE {temp_table_name} ('
    create_temp_table_sql += \
        ', '.join([f'{name} {type}' for name, type in columns]) + ');'

    print(create_temp_table_sql)
    cursor.execute(create_temp_table_sql)


def write_data_to_snowflake(
        df, table_name, columns, primary_keys,
        temporary_table_name, snowflake_conn
        ):
    '''
    Writes a Pandas DataFrame to a Snowflake table.

    Parameters:
    - df (pandas.DataFrame): DataFrame to be written to Snowflake.
    - table_name (str): Name of the target table in Snowflake.
    - primary_keys (list): List of columns to be used as primary keys.
    - columns (list): List of columns and their types for creating the table.
    - temporary_table_name (str): Name of the temporary table in Snowflake.
    - snowflake_conn (str): Snowflake connection ID.

    Utilizes the SnowflakeHook from Airflow to establish a connection.
    The write_pandas method from snowflake-connector-python is used to
    write the DataFrame.
    '''
    # Use the SnowflakeHook to get a connection object
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn)
    conn = hook.get_conn()
    cursor = conn.cursor()

    if df is None or df.empty:
        print('[Snowflake] Writing aborted beacause empty dataframe')
        return

    try:
        # Set timezone in Snowflake
        cursor.execute("ALTER SESSION SET TIMEZONE = 'America/Santiago'")

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
            raise Exception(f'Failed to write to {table_name} in Snowflake.')
        print(f'Successfully wrote {nrows} rows to '
              f'{temporary_table_name} in Snowflake.')

        # Generate UPDATE y INSERT by snowflake_shopify_customer_table_columns
        update_set_parts = []
        insert_columns = []
        insert_values = []

        for column, _ in columns:
            update_set_parts.append(
                f'{table_name}.{column} = new_data.{column}')
            insert_columns.append(column)
            insert_values.append(f'new_data.{column}')

        update_set_sql = ',\n'.join(update_set_parts)
        insert_columns_sql = ', '.join(insert_columns)
        insert_values_sql = ', '.join(insert_values)

        # Checking for duplicates in temporary table
        primary_key_conditions = \
            ' AND '.join([
                f'{table_name}.{pk} = new_data.{pk}' for pk in primary_keys
            ])
        primary_key_columns = ', '.join(primary_keys)

        cursor.execute(f'''
            SELECT {primary_key_columns}, COUNT(*)
            FROM {temporary_table_name}
            GROUP BY {primary_key_columns}
            HAVING COUNT(*) > 1''')
        temp_duplicates = cursor.fetchall()
        if temp_duplicates:
            duplicate_keys = \
                ', '.join([str(dup[0]) for dup in temp_duplicates])
            print(f'Duplicates keys: {duplicate_keys}')
            print(f'SELECT * FROM {temporary_table_name} '
                  f'WHERE {primary_key_columns} IN ({duplicate_keys})')
            cursor.execute(
                f'SELECT * FROM {temporary_table_name} '
                f'WHERE {primary_key_columns} IN ({duplicate_keys})'
            )
            duplicate_details = cursor.fetchall()
            print(f'Duplicates in temporary table: {temp_duplicates}')
            print(f'Duplicates details: {duplicate_details}')
            cursor.execute('ROLLBACK')
            return

        # Snowflake Merge execute
        cursor.execute('BEGIN')
        merge_sql = f'''
        MERGE INTO {table_name} USING {temporary_table_name} AS new_data
        ON {primary_key_conditions}
        WHEN MATCHED THEN
            UPDATE SET
                {update_set_sql},
                snowflake_updated_at = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns_sql},
                    snowflake_created_at, snowflake_updated_at)
            VALUES ({insert_values_sql},
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
         '''
        cursor.execute(merge_sql)

        duplicates = check_duplicates_sql(cursor, table_name, primary_keys)
        if duplicates:
            duplicate_keys = ', '.join([str(dup[0]) for dup in duplicates])
            cursor.execute(
                f'SELECT * FROM {table_name} '
                f'WHERE {primary_key_columns} IN ({duplicate_keys})'
            )
            duplicate_details = cursor.fetchall()

            cursor.execute('ROLLBACK')
            print(f'There are duplicates: {duplicates}. ROLLBACK executed.')
            print(f'Duplicate details: {duplicate_details}')
            raise ValueError(f'Duplicates found in {table_name}.')
        else:
            cursor.execute('COMMIT')
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
        cursor.execute('ROLLBACK')
        raise e
    finally:
        cursor.close()
        conn.close()


def check_duplicates_sql(cursor, table_name, primary_keys):
    '''
    Checks for duplicate records in a specified Snowflake table.

    This function executes an SQL query to identify duplicate entries
    based on the primary keys.

    Parameters:
    - cursor: A database cursor to execute the query in Snowflake.
    - table_name (str): The name of the table to check for duplicates.
    - primary_keys (list): The list of columns to check for duplicates.

    Returns:
    - list: A list of tuples containing the primary keys and the count
    of their occurrences, if duplicates are found.

    The function executes an SQL query that groups records by primary keys
    and counts occurrences, looking for counts greater than one.
    If duplicates are found, it returns the list of these records.
    In case of an exception, it performs a rollback and prints the error.
    '''
    primary_key_columns = ', '.join(primary_keys)
    check_duplicates_sql = f'''
    SELECT {primary_key_columns}, COUNT(*)
    FROM {table_name}
    GROUP BY {primary_key_columns}
    HAVING COUNT(*) > 1;
    '''
    try:
        cursor.execute(check_duplicates_sql)
        return cursor.fetchall()
    except Exception as e:
        cursor.execute('ROLLBACK')
        print('ROLLBACK executed due to an error:', e)


def fetch_data_from_snowflake(
        table_name, target_column, compare_column,
        start_date, end_date, snowflake_conn):
    # Use the SnowflakeHook to get a connection object
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn)
    conn = hook.get_conn()
    cursor = conn.cursor()

    query = f'''
    SELECT {target_column}
    FROM {table_name}
    WHERE DATE({compare_column}) >= '{start_date}'
    AND DATE({compare_column}) <= '{end_date}'
    '''
    # Executing the query
    ids_data_list = []
    cursor.execute(query)
    for row in cursor:
        ids_data_list.append(row[0])

    cursor.close()
    conn.close()
    return ids_data_list


def truncate_table(table_name, snowflake_conn_id):
    '''
    Truncates a specified table in Snowflake.

    Parameters:
    - table_name (str): Name of the table to truncate.
    - snowflake_conn_id (str): Airflow connection ID for Snowflake.

    This function uses the SnowflakeHook from Airflow to connect to Snowflake
    and executes the TRUNCATE TABLE command on the specified table.
    '''
    # Use the SnowflakeHook to get a connection object
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {table_name};")
        print(f"Table {table_name} truncated successfully.")
    except Exception as e:
        print(f"Error while truncating table {table_name}: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()
