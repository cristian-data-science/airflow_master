from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from dags.config.oms_incidence_data_config import default_args
from dags.utils.utils import write_data_to_snowflake
import os
import requests
from requests.exceptions import HTTPError, ChunkedEncodingError
import time
import pandas as pd

# Load environment variables from .env file
load_dotenv()
OMS_API_CLIENT_ID = os.getenv('OMS_API_CLIENT_ID')
OMS_API_CLIENT_SECRET = os.getenv('OMS_API_CLIENT_SECRET')
OMS_API_URL = os.getenv('OMS_API_URL')
OMS_API_INSTANCE = os.getenv('OMS_API_INSTANCE')
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')
OMS_TOKEN_URL = f'{OMS_API_URL}authentication/oauth2/token'
OMS_INCIDENCE_URL = f'{OMS_API_URL}{OMS_API_INSTANCE}/powerbi/incidence'

# Dag definition
dag = DAG(
    'oms_incidence_data',
    default_args=default_args,
    description='DAG to extract incidence data from OMS '
    'and write in Snowflake',
    schedule_interval='*/30 * * * *',
)


class OMSIncidenceDataFetcher:

    def __init__(self):
        print('Ejecutó init')
        self.auth_token = None
        self.get_auth_token()

    def get_auth_token(self):
        data = {
            'grant_type': 'client_credentials',
            'client_id': OMS_API_CLIENT_ID,
            'client_secret': OMS_API_CLIENT_SECRET
        }
        response = requests.post(OMS_TOKEN_URL, data=data)
        response.raise_for_status()
        self.auth_token = response.json()['access_token']

    def fetch_oms_incidences(self, batch_limit=20, total_limit=50,
                             db_write_batch_size=100, max_retries=5):
        headers = {'Authorization': f'Bearer {self.auth_token}'}
        incidences = []
        offset = 0
        requests_count = 0
        total_fetched = 0
        retries = 0

        while True:
            if not self.auth_token:
                self.get_auth_token()
                headers['Authorization'] = f'Bearer {self.auth_token}'

            try:
                filters = str([
                    ("ecommerce_name_child", "<>", False),
                    ("name", "<>", "Recalculate Delivery Date"),
                    ("name", "<>", False)
                ])
                params = {
                    'limit': batch_limit,
                    'offset': offset,
                    'filters': filters
                }
                prepared_url = requests.Request(
                    'GET', OMS_INCIDENCE_URL,
                    headers=headers, params=params).prepare().url
                print(f'Prepared URL: {prepared_url}')

                print(f'[OMS] Getting {batch_limit} incidences -'
                      f' offset: {offset}')
                print(f'[OMS] Request count: {requests_count}')
                response = requests.get(prepared_url, headers=headers)
                requests_count += 1

                response.raise_for_status()
                batch = response.json().get('data', [])
                incidences.extend(batch)
                total_fetched += len(batch)

                if len(incidences) >= db_write_batch_size:
                    print(f'To process {len(incidences)} incidences')
                    self.process_incidences(incidences)
                    incidences = []

                if not batch or (total_limit and total_fetched >= total_limit):
                    break

                offset += batch_limit
                time.sleep(1)

            except HTTPError as e:
                if e.response.status_code == 401:  # Unauthorized / Expired
                    print('Token expired. Obtaining new token...')
                    self.get_auth_token()
                    headers['Authorization'] = f'Bearer {self.auth_token}'
                    continue
                retries += 1
                if max_retries > 0:
                    print(f'Retrying... attempt {retries}')
                    time.sleep(2 ** retries)  # Exponential backoff
                else:
                    print('Max retries exceeded. Failing the task.')
                    raise e
            except (ConnectionError, ChunkedEncodingError) as e:
                print(f'Connection error encountered: {str(e)}')
                retries += 1
                if retries < max_retries:
                    print(f'Retrying... attempt {retries}')
                    time.sleep(2 ** retries)  # Exponential backoff
                else:
                    print('Max retries exceeded. Failing the task.')
                    raise e
            except Exception as e:
                print(f'Unhandled exception: {str(e)}')
                raise e

        if incidences:
            print(f'Processing the last batch of {len(incidences)} incidences')
            self.process_incidences(incidences)
        return incidences

    def process_incidences(self, incidences_list):
        orders_incidences_dataframe, incendences_history_dataframe = \
            self.incidences_to_dataframe(incidences_list)

        print('[AIRFLOW] Order-Incidence Dataframe: ')
        print(orders_incidences_dataframe.head().to_string())
        print('[AIRFLOW] Incidences History Dataframe: ')
        print(incendences_history_dataframe.head().to_string())

        write_data_to_snowflake(
            orders_incidences_dataframe,
            'OMS_ORDER_INCIDENCE',
            default_args['snowflake_oms_order_incidence_table_columns'],
            ['ECOMMERCE_NAME_CHILD'],
            'TEMP_OMS_ORDER_INCIDENCE',
            SNOWFLAKE_CONN_ID
        )

        write_data_to_snowflake(
            incendences_history_dataframe,
            'OMS_HISTORY_INCIDENCE',
            default_args['snowflake_oms_history_incidence_table_columns'],
            ['PRIMARY_KEY'],
            'TEMP_OMS_HISTORY_INCIDENCE',
            SNOWFLAKE_CONN_ID
        )

    def incidences_to_dataframe(self, incidences):
        all_history_data = []
        order_incidence_dict = {}

        for incidence in incidences:
            ecommerce_name_child = incidence['ecommerce_name_child']
            incidence_create_date = pd.to_datetime(incidence['create_date'])
            for history in incidence.get('history', []):
                history_create_date = pd.to_datetime(history['create_date'])
                history_name = history['name']
                history_entry = {
                    'PRIMARY_KEY': (
                        f'{ecommerce_name_child}-'
                        f'{history_create_date}-{history_name}'
                    ),
                    'ECOMMERCE_NAME_CHILD': ecommerce_name_child,
                    'CREATE_DATE': pd.to_datetime(history['create_date']),
                    'DESCRIPTION': history['description'],
                    'NAME': history['name'],
                    'STATE': history['state'],
                    'USER': history['user']
                }
                all_history_data.append(history_entry)

                '''Update order-incidence with the last register'''
                if (ecommerce_name_child not in order_incidence_dict
                        or history_entry[
                            'CREATE_DATE'] > order_incidence_dict[
                                ecommerce_name_child]['LAST_REGISTER_DATE']):
                    order_incidence_dict[ecommerce_name_child] = {
                        'ECOMMERCE_NAME_CHILD': ecommerce_name_child,
                        'INCIDENCE_CREATE_DATE': incidence_create_date,
                        'LAST_REGISTER_DATE': history_entry['CREATE_DATE'],
                        'DESCRIPTION': history_entry['DESCRIPTION'],
                        'NAME': history_entry['NAME'],
                        'STATE': history_entry['STATE'],
                        'USER': history_entry['USER']
                    }

        df_order_incidence = \
            pd.DataFrame(list(order_incidence_dict.values()))
        df_history = pd.DataFrame(all_history_data)
        df_history['CREATE_DATE'] = \
            df_history['CREATE_DATE'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_order_incidence['INCIDENCE_CREATE_DATE'] = \
            df_order_incidence[
                'INCIDENCE_CREATE_DATE'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_order_incidence['LAST_REGISTER_DATE'] = \
            df_order_incidence[
                'LAST_REGISTER_DATE'].dt.strftime('%Y-%m-%d %H:%M:%S')
        return df_order_incidence, df_history


def run_get_oms_incidences(**context):
    execution_date = context['execution_date']
    print(f'Execution Date: {execution_date}')
    fetcher = OMSIncidenceDataFetcher()
    fetcher.fetch_oms_incidences(
        batch_limit=300, total_limit=600,
        db_write_batch_size=300, max_retries=5
    )


# Task definitions
task_1 = PythonOperator(
    task_id='get_oms_incidences',
    python_callable=run_get_oms_incidences,
    dag=dag,
)
