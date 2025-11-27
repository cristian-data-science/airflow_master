from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from dotenv import load_dotenv
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from config.carbonfootprint_exitdocs_config import (
    default_args,
    carbonfootprint_exitdocs_columns,
    DISTANCE_TABLE,
    WAREHOUSE_TO_CITY
)
from utils.utils import write_data_to_snowflake
import os
import pandas as pd
import logging

load_dotenv()

# Environment variables
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')

AVERAGE_PRODUCT_WEIGHT_KG = \
      float(Variable.get(
          "carbonfootprint_average_product_weight_kg",
          default_var="0.32"))
DEFAULT_EMISSIONS_FACTOR = \
    float(Variable.get(
        "carbonfootprint_default_emissions_factor",
        default_var="0.12418"))

# Airflow Variables for date range (format: YYYY-MM-DD)
# Can be configured from Airflow UI
# carbonfootprint_start_date: start date of the range
# carbonfootprint_end_date: end date of the range
# carbonfootprint_days_lookback: days lookback (alternative to fixed dates)

# DAG definition
dag = DAG(
    'carbonfootprint_exitdocs',
    default_args=default_args,
    description='DAG to calculate carbon footprint from exit documents '
                '(Transfer Orders from CD and Wholesale orders)',
    schedule_interval=None,  # Manual trigger - runs with date range
    catchup=False,
    tags=['carbon_footprint', 'transfer_orders', 'wholesale']
)


def get_date_range():
    """
    Gets the date range for the query.
    Priority:
    1. Both start_date and end_date variables provided
    2. Only start_date provided -> end_date defaults to today
    3. Only end_date provided -> start_date uses days_lookback
    4. Neither provided -> use days_lookback from today
    """
    try:
        start_date_str = Variable.get(
            "carbonfootprint_start_date", default_var=None)
        end_date_str = Variable.get(
            "carbonfootprint_end_date", default_var=None)

        # Case 1: Both dates provided
        if start_date_str and end_date_str:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            logging.info(
                f"Using date range from Variables: "
                f"{start_date_str} to {end_date_str}"
            )
            return start_date, end_date

        # Case 2: Only start_date provided -> end_date = today
        if start_date_str:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.now()
            logging.info(
                f"Using start_date from Variable, end_date = today: "
                f"{start_date_str} to {end_date.strftime('%Y-%m-%d')}"
            )
            return start_date, end_date

        # Case 3: Only end_date provided -> use days_lookback
        if end_date_str:
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            days_lookback = int(Variable.get(
                "carbonfootprint_days_lookback", default_var="30"))
            start_date = end_date - timedelta(days=days_lookback)
            logging.info(
                f"Using end_date from Variable with {days_lookback} days "
                f"lookback: {start_date.strftime('%Y-%m-%d')} to "
                f"{end_date_str}"
            )
            return start_date, end_date

    except Exception as e:
        logging.warning(f"Error parsing date variables: {e}")

    # Case 4: Fallback to days lookback from today
    days_lookback = int(Variable.get(
        "carbonfootprint_days_lookback", default_var="30"))
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_lookback)

    logging.info(
        f"Using days lookback ({days_lookback} days): "
        f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
    )
    return start_date, end_date


def get_emissions_factor():
    """
    Gets the emissions factor from Airflow Variable or uses the default.
    """
    try:
        factor = float(Variable.get(
            "carbonfootprint_emissions_factor",
            default_var=str(DEFAULT_EMISSIONS_FACTOR)
        ))
        logging.info(f"Using emissions factor: {factor}")
        return factor
    except Exception:
        logging.info(
            f"Using default emissions factor: {DEFAULT_EMISSIONS_FACTOR}"
        )
        return DEFAULT_EMISSIONS_FACTOR


def get_transfer_orders_from_cd(start_date, end_date):
    """
    Queries Transfer Orders departing from CD
    (SHIPPING_WAREHOUSE_ID = 'CD') within the date range.
    """
    logging.info(
        f"[Snowflake] Fetching TR headers from CD between "
        f"{start_date.strftime('%Y-%m-%d')} and "
        f"{end_date.strftime('%Y-%m-%d')}"
    )

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    query = f"""
    SELECT
        TRANSFER_ORDER_NUMBER,
        RECEIVING_WAREHOUSE_ID,
        RECEIVING_ADDRESS_CITY,
        RECEIVING_ADDRESS_STREET,
        RECEIVING_ADDRESS_DISTRICT_NAME,
        REQUESTED_SHIPPING_DATE
    FROM PATAGONIA.CORE_TEST.ERP_TRANSFER_ORDERS
    WHERE SHIPPING_WAREHOUSE_ID = 'CD'
      AND REQUESTED_SHIPPING_DATE >= '{start_date.strftime('%Y-%m-%d')}'
      AND REQUESTED_SHIPPING_DATE <= '{end_date.strftime('%Y-%m-%d')} 23:59:59'
    """

    logging.info(f"[Snowflake] Query: {query}")

    df = hook.get_pandas_df(query)
    logging.info(
        f"[Snowflake] Found {len(df)} TR headers from CD in date range"
    )

    if not df.empty:
        logging.info(f"[Snowflake] Sample data:\n{df.head()}")

    return df


def get_transfer_order_lines(tr_numbers):
    """
    Queries Transfer Order lines to calculate the total
    quantity of products.
    Excludes:
    - TRs with no lines
    - TRs that contain the item 'TRASLADO'
    """
    if not tr_numbers:
        logging.warning("[Snowflake] No TR numbers provided for lines query")
        return pd.DataFrame()

    logging.info(
        f"[Snowflake] Fetching lines for {len(tr_numbers)} TRs"
    )

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    # Create TR list for IN clause
    tr_list = "', '".join(tr_numbers)

    query = f"""
    SELECT
        TRANSFER_ORDER_NUMBER,
        SUM(TRANSFER_QUANTITY) as TOTAL_QUANTITY
    FROM PATAGONIA.CORE_TEST.ERP_TRANSFER_ORDERS_LINES
    WHERE TRANSFER_ORDER_NUMBER IN ('{tr_list}')
      AND UPPER(ITEM_NUMBER) != 'TRASLADO'
      AND TRANSFER_ORDER_NUMBER NOT IN (
          SELECT DISTINCT TRANSFER_ORDER_NUMBER
          FROM PATAGONIA.CORE_TEST.ERP_TRANSFER_ORDERS_LINES
          WHERE UPPER(ITEM_NUMBER) = 'TRASLADO'
      )
    GROUP BY TRANSFER_ORDER_NUMBER
    HAVING SUM(TRANSFER_QUANTITY) > 0
    """

    logging.info(f"[Snowflake] Lines query for {len(tr_numbers)} TRs")
    logging.info(f"[Snowflake] Query: {query}")

    df = hook.get_pandas_df(query)
    logging.info(
        f"[Snowflake] Found quantities for {len(df)} TRs "
        f"(excluding TRASLADO and empty TRs)"
    )

    if not df.empty:
        logging.info(f"[Snowflake] Sample quantities:\n{df.head()}")

    return df


def get_distance_for_city(city_name):
    """
    Gets the distance from Pudahuel to a destination city.
    Tries exact match first, then searches for partial matches.
    """
    if not city_name:
        logging.warning("No city name provided for distance lookup")
        return None, None

    # Normalize city name
    city_normalized = city_name.strip()

    # Exact search
    if city_normalized in DISTANCE_TABLE:
        return DISTANCE_TABLE[city_normalized]

    # Case-insensitive search
    city_lower = city_normalized.lower()
    for city, (zip_code, distance) in DISTANCE_TABLE.items():
        if city.lower() == city_lower:
            return (zip_code, distance)

    # Partial search (if city name is contained)
    for city, (zip_code, distance) in DISTANCE_TABLE.items():
        if city_lower in city.lower() or city.lower() in city_lower:
            logging.info(
                f"Partial match found: '{city_name}' -> '{city}'"
            )
            return (zip_code, distance)

    logging.warning(
        f"No distance found for city: '{city_name}'. Using default 10 km."
    )
    return ('0000000', 10.0)  # Default for cities not found


def get_wholesale_orders(start_date, end_date):
    """
    Queries Wholesale orders from ERP_PROCESSED_SALESLINE.
    Groups by PURCHORDERFORMNUM and sums QTY (only positive quantities).
    Excludes:
    - Orders with no lines (QTY <= 0)
    - Orders that contain the item 'TRASLADO'
    """
    logging.info(
        f"[Snowflake] Fetching Wholesale orders between "
        f"{start_date.strftime('%Y-%m-%d')} and "
        f"{end_date.strftime('%Y-%m-%d')}"
    )

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    query = f"""
    SELECT
        PURCHORDERFORMNUM,
        SUM(QTY) as TOTAL_QUANTITY,
        MIN(DATE(CREATEDTRANSACTIONDATE2)) as ORDER_DATE
    FROM PATAGONIA.CORE_TEST.ERP_PROCESSED_SALESLINE
    WHERE CANAL = 'WholeSale'
      AND DATE(CREATEDTRANSACTIONDATE2) >= '{start_date.strftime('%Y-%m-%d')}'
      AND DATE(CREATEDTRANSACTIONDATE2) <= '{end_date.strftime('%Y-%m-%d')}'
      AND QTY > 0
      AND PURCHORDERFORMNUM IS NOT NULL
      AND TRIM(PURCHORDERFORMNUM) != ''
      AND UPPER(ITEMID) != 'TRASLADO'
      AND PURCHORDERFORMNUM NOT IN (
          SELECT DISTINCT PURCHORDERFORMNUM
          FROM PATAGONIA.CORE_TEST.ERP_PROCESSED_SALESLINE
          WHERE CANAL = 'WholeSale'
            AND UPPER(ITEMID) = 'TRASLADO'
            AND PURCHORDERFORMNUM IS NOT NULL
      )
    GROUP BY PURCHORDERFORMNUM
    HAVING SUM(QTY) > 0
    """

    logging.info(f"[Snowflake] Wholesale Query: {query}")

    df = hook.get_pandas_df(query)
    logging.info(
        f"[Snowflake] Found {len(df)} Wholesale orders in date range "
        f"(excluding TRASLADO and empty orders)"
    )

    if not df.empty:
        logging.info(f"[Snowflake] Sample Wholesale data:\n{df.head()}")

    return df


def get_city_from_warehouse(warehouse_id, receiving_city, district_name):
    """
    Determines the destination city based on warehouse_id,
    receiving city or district.
    """
    # First try with warehouse mapping
    if warehouse_id and warehouse_id.upper() in WAREHOUSE_TO_CITY:
        return WAREHOUSE_TO_CITY[warehouse_id.upper()]

    # Then use district (more specific)
    if district_name:
        # Clean the district
        district_clean = district_name.strip()
        if district_clean in DISTANCE_TABLE:
            return district_clean

    # Finally use the city
    if receiving_city:
        city_clean = receiving_city.strip()
        if city_clean in DISTANCE_TABLE:
            return city_clean

    # Default
    logging.warning(
        f"Could not determine city for warehouse: {warehouse_id}, "
        f"city: {receiving_city}, district: {district_name}"
    )
    return receiving_city or district_name or 'Unknown'


def calculate_carbon_footprint(**context):
    """
    Main function that calculates the carbon footprint for exit TRs
    and Wholesale orders.
    """
    logging.info("[Start] Calculating Carbon Footprint for Exit Documents")

    # Get date range
    start_date, end_date = get_date_range()

    # Get emissions factor
    emissions_factor = get_emissions_factor()

    records = []
    errors_count = 0
    tr_count = 0
    wholesale_count = 0

    # =========================================================================
    # PART 1: Process Transfer Orders from CD
    # =========================================================================
    logging.info("=" * 50)
    logging.info("Processing Transfer Orders from CD")
    logging.info("=" * 50)

    # 1. Get TRs from CD
    df_headers = get_transfer_orders_from_cd(start_date, end_date)

    if not df_headers.empty:
        # 2. Get quantities per TR
        tr_numbers = df_headers['TRANSFER_ORDER_NUMBER'].tolist()
        df_lines = get_transfer_order_lines(tr_numbers)

        if not df_lines.empty:
            # 3. Merge headers with lines (inner join - excludes TRs w/o lines)
            df_merged = df_headers.merge(
                df_lines,
                on='TRANSFER_ORDER_NUMBER',
                how='inner'
            )

            logging.info(
                f"Merged TR data has {len(df_merged)} records "
                f"(after filtering TRs without lines)"
            )

            # 4. Calculate carbon footprint for each TR
            for idx, row in df_merged.iterrows():
                try:
                    tr_number = row['TRANSFER_ORDER_NUMBER']

                    # Determine destination city
                    destination_city = get_city_from_warehouse(
                        row.get('RECEIVING_WAREHOUSE_ID'),
                        row.get('RECEIVING_ADDRESS_CITY'),
                        row.get('RECEIVING_ADDRESS_DISTRICT_NAME')
                    )

                    # Get distance and zip code
                    zip_code, distance = get_distance_for_city(
                        destination_city
                    )

                    if zip_code is None:
                        zip_code = '0000000'
                        distance = 10.0

                    # Calculate weight (ensure correct Decimal conversion)
                    total_quantity = row.get('TOTAL_QUANTITY', 0) or 0
                    weight_kg = (
                        Decimal(str(total_quantity)) *
                        Decimal(str(AVERAGE_PRODUCT_WEIGHT_KG))
                    )
                    weight_kg = weight_kg.quantize(
                        Decimal('0.01'), rounding=ROUND_HALF_UP
                    )

                    # Convert distance to Decimal with 2 decimal places
                    distance_decimal = Decimal(str(distance)).quantize(
                        Decimal('0.01'), rounding=ROUND_HALF_UP
                    )

                    # Convert emissions factor to Decimal with 6 decimal places
                    emissions_factor_decimal = Decimal(
                        str(emissions_factor)
                    ).quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP)

                    # DESTINATION_ADDRESS = destination city/district name
                    destination_address = destination_city

                    # Get date
                    shipping_date = row.get('REQUESTED_SHIPPING_DATE')
                    if pd.notna(shipping_date):
                        if isinstance(shipping_date, str):
                            fecha = datetime.strptime(
                                shipping_date[:10], '%Y-%m-%d'
                            ).date()
                        else:
                            fecha = shipping_date.date() if hasattr(
                                shipping_date, 'date') else shipping_date
                    else:
                        fecha = None

                    record = {
                        'ORIGIN_NAME': 'Pudahuel',
                        'ORIGIN_ZIP_CODE': '9020000',
                        'CHANNEL': 'Retail',
                        'DESTINATION_ZIP_CODE': zip_code,
                        'DESTINATION_ADDRESS': (
                            destination_address[:500]
                            if destination_address else ''
                        ),
                        'MODE': 'Ground',
                        'DISTANCE_KM': float(distance_decimal),
                        'WEIGHT_KG': float(weight_kg),
                        'EMISSIONS_FACTOR': float(emissions_factor_decimal),
                        'IS_ELECTRIC': 'NO',
                        'FECHA': fecha,
                        'REFERENCE_NUMBER': tr_number,
                    }

                    records.append(record)
                    tr_count += 1

                    logging.debug(
                        f"[OK] TR {tr_number} -> {destination_city}: "
                        f"{distance:.2f} km, {weight_kg:.2f} kg"
                    )

                except Exception as e:
                    errors_count += 1
                    logging.error(
                        f"[Error] Processing TR "
                        f"{row.get('TRANSFER_ORDER_NUMBER')}: {str(e)}"
                    )
        else:
            logging.warning("No lines found for the Transfer Orders")
    else:
        logging.warning("No Transfer Orders found for the date range")

    logging.info(f"Processed {tr_count} Transfer Orders")

    # =========================================================================
    # PART 2: Process Wholesale Orders
    # =========================================================================
    logging.info("=" * 50)
    logging.info("Processing Wholesale Orders")
    logging.info("=" * 50)

    # Get Wholesale orders
    df_wholesale = get_wholesale_orders(start_date, end_date)

    if not df_wholesale.empty:
        # Get distance from Pudahuel to Providencia (fixed destination)
        providencia_zip, providencia_distance = get_distance_for_city(
            'Providencia'
        )

        # Convert to Decimal
        distance_decimal = Decimal(str(providencia_distance)).quantize(
            Decimal('0.01'), rounding=ROUND_HALF_UP
        )
        emissions_factor_decimal = Decimal(str(emissions_factor)).quantize(
            Decimal('0.000001'), rounding=ROUND_HALF_UP
        )

        for idx, row in df_wholesale.iterrows():
            try:
                order_number = row['PURCHORDERFORMNUM']
                total_quantity = row.get('TOTAL_QUANTITY', 0) or 0
                order_date = row.get('ORDER_DATE')

                # Calculate weight
                weight_kg = (
                    Decimal(str(total_quantity)) *
                    Decimal(str(AVERAGE_PRODUCT_WEIGHT_KG))
                )
                weight_kg = weight_kg.quantize(
                    Decimal('0.01'), rounding=ROUND_HALF_UP
                )

                # Get date
                if pd.notna(order_date):
                    if isinstance(order_date, str):
                        fecha = datetime.strptime(
                            order_date[:10], '%Y-%m-%d'
                        ).date()
                    else:
                        fecha = order_date.date() if hasattr(
                            order_date, 'date') else order_date
                else:
                    fecha = None

                record = {
                    'ORIGIN_NAME': 'Pudahuel',
                    'ORIGIN_ZIP_CODE': '9020000',
                    'CHANNEL': 'Wholesale',
                    'DESTINATION_ZIP_CODE': providencia_zip,
                    'DESTINATION_ADDRESS': 'Providencia',
                    'MODE': 'Ground',
                    'DISTANCE_KM': float(distance_decimal),
                    'WEIGHT_KG': float(weight_kg),
                    'EMISSIONS_FACTOR': float(emissions_factor_decimal),
                    'IS_ELECTRIC': 'NO',
                    'FECHA': fecha,
                    'REFERENCE_NUMBER': order_number,
                }

                records.append(record)
                wholesale_count += 1

                logging.debug(
                    f"[OK] Wholesale {order_number}: "
                    f"{providencia_distance:.2f} km, {weight_kg:.2f} kg"
                )

            except Exception as e:
                errors_count += 1
                logging.error(
                    f"[Error] Processing Wholesale order "
                    f"{row.get('PURCHORDERFORMNUM')}: {str(e)}"
                )
    else:
        logging.warning("No Wholesale orders found for the date range")

    logging.info(f"Processed {wholesale_count} Wholesale Orders")

    # =========================================================================
    # PART 3: Write all records to Snowflake
    # =========================================================================
    logging.info("=" * 50)
    logging.info("Writing all records to Snowflake")
    logging.info("=" * 50)

    logging.info(
        f"Total processed: {len(records)} records "
        f"({tr_count} TRs + {wholesale_count} Wholesale), "
        f"{errors_count} errors"
    )

    if not records:
        logging.warning("No records to write to Snowflake")
        return {'records_processed': 0, 'errors': errors_count}

    # Create DataFrame and write to Snowflake
    df_output = pd.DataFrame(records)

    # Convert date to string for Snowflake
    df_output['FECHA'] = pd.to_datetime(
        df_output['FECHA'], errors='coerce'
    ).dt.strftime('%Y-%m-%d')

    logging.info(f"Output DataFrame:\n{df_output.head(10)}")
    logging.info(f"DataFrame shape: {df_output.shape}")

    # Write to Snowflake
    write_data_to_snowflake(
        df_output,
        'CARBONFOOTPRINT_EXITDOCS',
        carbonfootprint_exitdocs_columns,
        ['REFERENCE_NUMBER'],  # Unique key
        'TEMP_CARBONFOOTPRINT_EXITDOCS',
        SNOWFLAKE_CONN_ID
    )

    logging.info(
        f"[Success] Wrote {len(df_output)} records to "
        "CARBONFOOTPRINT_EXITDOCS"
    )

    # Final summary
    total_distance = df_output['DISTANCE_KM'].sum()
    total_weight = df_output['WEIGHT_KG'].sum()

    logging.info(
        f"\n{'='*50}\n"
        f"CARBON FOOTPRINT SUMMARY\n"
        f"{'='*50}\n"
        f"Date Range: {start_date.strftime('%Y-%m-%d')} to "
        f"{end_date.strftime('%Y-%m-%d')}\n"
        f"Transfer Orders Processed: {tr_count}\n"
        f"Wholesale Orders Processed: {wholesale_count}\n"
        f"Total Records: {len(df_output)}\n"
        f"Total Distance (km): {total_distance:,.2f}\n"
        f"Total Weight (kg): {total_weight:,.2f}\n"
        f"Emissions Factor: {emissions_factor}\n"
        f"{'='*50}"
    )

    return {
        'records_processed': len(df_output),
        'tr_count': tr_count,
        'wholesale_count': wholesale_count,
        'errors': errors_count,
        'total_distance_km': total_distance,
        'total_weight_kg': total_weight
    }


# Task
task_calculate_footprint = PythonOperator(
    task_id='calculate_carbon_footprint',
    python_callable=calculate_carbon_footprint,
    dag=dag,
)
