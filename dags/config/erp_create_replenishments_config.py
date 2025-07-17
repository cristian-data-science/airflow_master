from datetime import timedelta
from airflow.utils.dates import days_ago

# Default variables
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
    'email': ['enrique.urrutia@patagonia.com'],
    'snowflake_conn_id': 'patagonia_snowflake_connection',
}

# DAG configuration
dag_config = {
    'dag_id': 'erp_create_replenishments',
    'description': 'Create ERP replenishments from Snowflake data',
    'schedule_interval': None,  # Only triggered externally
    'catchup': False,
    'tags': ['erp', 'replenishment'],
    # Prevent concurrent executions to avoid conflicts
    'max_active_runs': 1,  # Only allow 1 DAG run at a time
    'max_active_tasks': 1,  # Only allow 1 task at a time
    # List of email addresses to notify
    'notification_emails': [
        'enrique.urrutia@patagonia.com',
        'sebastian.bahamondes@patagonia.com',
        'juan.valdes@patagonia.com',
        'zdenka.skorin@patagonia.com'
    ]
}
