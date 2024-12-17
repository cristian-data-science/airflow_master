from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from dags.config.exchange_rates_config import default_args
import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()


def get_dollar_value():
    """
    Obtains the current dollar value from the mindicador.cl API.
    """
    try:
        resp = requests.get("https://mindicador.cl/api/dolar")
        data = resp.json()
        last_dollar_value = data['serie'][0]['valor']
        print(f"The last dollar value is: {last_dollar_value}")
        return last_dollar_value
    except Exception as e:
        print(f"Error obtaining the dollar value: {e}")
        raise


def write_exchange_rate_to_erp():
    """
    Loads the exchange rate (USD to CLP) to the ERP using the obtained value.
    """
    erp_url = os.getenv('ERP_URL')
    erp_token_url = os.getenv('ERP_TOKEN_URL')
    erp_client_id = os.getenv('ERP_CLIENT_ID')
    erp_client_secret = os.getenv('ERP_CLIENT_SECRET')

    # Validate that all required variables are defined
    if not all([erp_url, erp_token_url, erp_client_id, erp_client_secret]):
        raise ValueError(
            "Missing required environment variables to connect to the ERP.")

    # Get access token
    token_url = f'{erp_token_url}/oauth2/v2.0/token'
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': erp_client_id,
        'client_secret': erp_client_secret,
        'scope': f'{erp_url}/.default'
    }
    token_headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    try:
        response = requests.post(
            token_url, data=token_data, headers=token_headers)
        if response.status_code == 200:
            access_token = response.json().get('access_token')
            print("Access Token:", access_token)
        else:
            raise Exception(
                f"Error obtaining token:{response.status_code}"
                f"- {response.text}")
    except Exception as e:
        print(f"Error during authentication: {e}")
        raise

    # Call function to get dollar value
    last_dollar_value = get_dollar_value()

    # Get current UTC date
    fecha_actual = datetime.utcnow()
    fecha_formateada = fecha_actual.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"Formatted date: {fecha_formateada}")

    # Prepare data to send to ERP
    exchange_url = f'{erp_url}/data/ExchangeRates'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    exchange_data = {
        "RateTypeName": "Predeterminado",
        "FromCurrency": "USD",
        "ToCurrency": "CLP",
        "StartDate": fecha_formateada,
        "Rate": last_dollar_value,
        "ConversionFactor": "One",
        "RateTypeDescription": "Tipo de cambio creado desde API"
    }

    try:
        response = requests.post(
            exchange_url, headers=headers, json=exchange_data)
        if response.status_code == 201:
            print("Information successfully written to the ERP.")
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


# DAG configuration
with DAG(
    'write_exchange_rate_dag',
    default_args=default_args,
    description=(
        'DAG that retrieves the dollar value and uploads it to the ERP'),
    schedule_interval='0 10 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    # Task 1: Get dollar value
    get_dollar_value_task = PythonOperator(
        task_id='get_dollar_value_task',
        python_callable=get_dollar_value
    )

    # Task 2: Write dollar value in ERP
    write_exchange_rate_to_erp_task = PythonOperator(
        task_id='write_exchange_rate_to_erp_task',
        python_callable=write_exchange_rate_to_erp
    )

    get_dollar_value_task >> write_exchange_rate_to_erp_task
