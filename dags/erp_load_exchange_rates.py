from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from config.erp_load_exchange_rates_config import default_args
import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

ERP_URL = os.getenv('ERP_URL')
ERP_TOKEN_URL = os.getenv('ERP_TOKEN_URL')
ERP_CLIENT_ID = os.getenv('ERP_CLIENT_ID')
ERP_CLIENT_SECRET = os.getenv('ERP_CLIENT_SECRET')


# Get current UTC date
actual_date = datetime.utcnow()
formatted_date = actual_date.strftime("%Y-%m-%dT%H:%M:%SZ")


def get_exchange_value(exchange_name):
    """
    Obtains the current exchange value from the mindicador.cl API.
    """
    try:
        resp = requests.get(f"https://mindicador.cl/api/{exchange_name}")
        data = resp.json()
        print(f"Data obtained from the API: {data}")
        last_exchange_value = data['serie'][0]['valor']
        print(f"The last {exchange_name} value is: {last_exchange_value}")
        return last_exchange_value
    except Exception as e:
        print(f"Error obtaining the {exchange_name} value: {e}")
        raise


def write_exchange_rate_to_erp(exchange_value, exchange_code, type):
    """
    Loads the exchange rate to the ERP using the obtained value.
    """

    # Validate that all required variables are defined
    if not all([ERP_URL, ERP_TOKEN_URL, ERP_CLIENT_ID, ERP_CLIENT_SECRET]):
        raise ValueError(
            "Missing required environment variables to connect to the ERP.")

    # Get access token
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
            token_url, data=token_data, headers=token_headers)
        if response.status_code == 200:
            access_token = response.json().get('access_token')
        else:
            raise Exception(
                f"Error obtaining token:{response.status_code}"
                f"- {response.text}")
    except Exception as e:
        print(f"Error during authentication: {e}")
        raise

    # Prepare data to send to ERP
    exchange_url = f'{ERP_URL}/data/ExchangeRates'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    exchange_data = {
        "RateTypeName": type,
        "FromCurrency": exchange_code,
        "ToCurrency": "CLP",
        "StartDate": formatted_date,
        "Rate": exchange_value,
        "ConversionFactor": "One",
        "RateTypeDescription": "Tipo de cambio creado desde API"
    }
    try:
        response = requests.post(
            exchange_url, headers=headers, json=exchange_data)
        if response.status_code == 201:
            print("Information successfully written to the ERP:"
                  f" {exchange_value} - {exchange_code} - {type}")
        else:
            error_msg = (
                f"Error writing to ERP: {response.status_code}"
                f"- {response.json()}"
            )
            print(error_msg)
            raise Exception(error_msg)
    except Exception as e:
        print(f"Error sending data to ERP: {e}")
        raise


last_day = ["01-31", "02-28", "03-31", "04-30", "05-31", "06-30", "07-31",
            "08-31", "09-30", "10-31", "11-30", "12-31"]


def get_and_write_exchange_rate(exchange_name, exchange_code):
    """
    Obtains the exchange rate and uploads it to the ERP.
    """
    try:
        print(f"Obtaining the {exchange_name} value.")
        last_exchange_value = get_exchange_value(exchange_name)
    except Exception as e:
        error_msg = f"Error obtaining the {exchange_name} value: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)
    print(f"Uploading the {exchange_name} value to the ERP.")
    try:
        write_exchange_rate_to_erp(
            last_exchange_value, exchange_code, "Predeterminado")
    except Exception as e:
        error_msg = (
            f"Error uploading exchange rate 'Predeterminado'"
            f" for {exchange_code} to ERP: {str(e)}"
        )
        print(error_msg)
        raise Exception(error_msg)

    try:
        write_exchange_rate_to_erp(
            last_exchange_value, exchange_code, "Tienda")
    except Exception as e:
        error_msg = (
            "Error uploading exchange rate 'Tienda' "
            f"for {exchange_code} to ERP: {str(e)}"
        )
        print(error_msg)
        raise Exception(error_msg)

    try:
        for c in last_day:
            if c in formatted_date:
                print("It is the last day of the month.")
                write_exchange_rate_to_erp(
                    last_exchange_value, exchange_code, "Cierre")
    except Exception as e:
        error_msg = (
            "Error verifying the last day of the month or "
            f"uploading for {exchange_code} 'Cierre': {str(e)}"
        )
        print(error_msg)
        raise Exception(error_msg)


def execute_exchange_rate_tasks():
    """
    Executes the tasks to obtain the exchange values and upload it to the ERP.
    """
    get_and_write_exchange_rate("dolar", "USD")
    get_and_write_exchange_rate("euro", "EUR")
    get_and_write_exchange_rate("uf", "CLF")


# DAG configuration
with DAG(
    'erp_load_exchange_rate_dag',
    default_args=default_args,
    description=(
        'DAG that retrieves the dollar value and uploads it to the ERP'),
    schedule_interval='0 10 * * *',
    catchup=False
) as dag:

    execute_exchange_rate_task = PythonOperator(
        task_id='execute_exchange_rate_task',
        python_callable=execute_exchange_rate_tasks
    )
