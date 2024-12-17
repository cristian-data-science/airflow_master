from datetime import timedelta, datetime

# Define DAG and task in Airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2024, 12, 16, 5, 0),
    'email': ['enrique.urrutia@patagonia.com'],
    'phone': 0,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}
