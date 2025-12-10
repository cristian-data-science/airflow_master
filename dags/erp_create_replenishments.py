from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from config.erp_create_replenishments_config import default_args, dag_config
import requests
import pandas as pd
import os
from datetime import datetime
import pytz
import logging
import random
from typing import List, Dict, Any
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Gmail SMTP configuration (from .env)
GMAIL_USER = os.getenv('GMAIL_USER')
GMAIL_PASS = os.getenv('GMAIL_PASS')
EMAIL_FROM = os.getenv('EMAIL_FROM', GMAIL_USER)


def send_email_via_gmail(to_emails, subject, html_content):
    """
    Sends an email using Gmail SMTP.

    Args:
        to_emails: List of recipient email addresses
        subject: Email subject
        html_content: HTML content of the email
    """
    if not GMAIL_USER or not GMAIL_PASS:
        raise ValueError(
            "Gmail credentials not configured. "
            "Please set GMAIL_USER and GMAIL_PASS in .env"
        )

    # Create message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = EMAIL_FROM
    msg['To'] = ', '.join(to_emails)

    # Attach HTML content
    html_part = MIMEText(html_content, 'html', 'utf-8')
    msg.attach(html_part)

    try:
        # Connect to Gmail SMTP server
        logging.info("Connecting to Gmail SMTP server...")
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(GMAIL_USER, GMAIL_PASS)

        # Send email
        server.sendmail(EMAIL_FROM, to_emails, msg.as_string())
        server.quit()

        logging.info(
            f"Email sent successfully via Gmail to {len(to_emails)} recipients"
        )
        return True

    except smtplib.SMTPAuthenticationError as e:
        logging.error(f"Gmail authentication failed: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to send email via Gmail: {e}")
        raise


ID_WAREHOUSE_WMS = Variable.get("wms_id_warehouse", default_var="04")
WMS_OWNER = "PATAGONIA"

# Store addresses dict
STORE_DELIVERY_INFO = {
    "BNAVENTURA": {
        "region": "RM",
        "comuna": "Quilicura",
        "ciudad": "Santiago",
        "direccion": "San Ignacio 500. Local 12-H",
    },
    "MALLSPORT": {
        "region": "RM",
        "comuna": "Las Condes",
        "ciudad": "Santiago",
        "direccion": "Avda. Las Condes 13451. Local 225",
    },
    "PTOVARAS": {
        "region": "X",
        "comuna": "Puerto Varas",
        "ciudad": "Puerto Varas",
        "direccion": "San José 192",
    },
    "TEMUCO": {
        "region": "IX",
        "comuna": "Temuco",
        "ciudad": "Temuco",
        "direccion": "Avda. Alemania 0671. Local 3011",
    },
    "ALERCE": {
        "region": "X",
        "comuna": "Puerto Montt",
        "ciudad": "Puerto Montt",
        "direccion": "local 127 - 128",
    },
    "CONCEPCION": {
        "region": "VIII",
        "comuna": "Concepción",
        "ciudad": "Concepción",
        "direccion": "Avda. Jorge Alessandri 3177. Local f-114. f-116",
    },
    "COYHAIQUE": {
        "region": "XI",
        "comuna": "Coyhaique",
        "ciudad": "Coyhaique",
        "direccion": "Calle Plaza 485",
    },
    "LADEHESA": {
        "region": "RM",
        "comuna": "Lo Barnechea",
        "ciudad": "Santiago",
        "direccion": "Avda. La Dehesa 1445. Local 2074",
    },
    "LASCONDES": {
        "region": "RM",
        "comuna": "Las Condes",
        "ciudad": "Santiago",
        "direccion": "Avda. Presidente Kennedy 9001. Local 3024",
    },
    "OSORNO": {
        "region": "X",
        "comuna": "Osorno",
        "ciudad": "Osorno",
        "direccion": "Juan Mackenna 1069",
    },
    "PUCON": {
        "region": "IX",
        "comuna": "Pucón",
        "ciudad": "Pucón",
        "direccion": "Fresia 248. Local C",
    },
    "COSTANERA": {
        "region": "RM",
        "comuna": "Providencia",
        "ciudad": "Santiago",
        "direccion": "Avenida Andres Bello 2425. piso 4. local 4169",
    },
}


# Testing configuration - enables realistic error simulation
TESTING_CONFIG = {
    'enable_error_simulation': Variable.get(
        "erp_enable_error_simulation", default_var="false"
    ).lower() == "true",
    'error_probabilities': {
        '401': float(Variable.get(
            "erp_error_401_probability", default_var="0.00")),  # Token expired
        '429': float(Variable.get(
            "erp_error_429_probability", default_var="0.00")),  # Rate limiting
        '500': float(Variable.get(
            "erp_error_500_probability", default_var="0.00")),  # Server error
        '502': float(Variable.get(
            "erp_error_502_probability", default_var="0.00")),  # Bad gateway
        '503': float(Variable.get(
            "erp_error_503_probability", default_var="0.00")),  # Unavailable
        '504': float(Variable.get(
            "erp_error_504_probability", default_var="0.00")),  # Timeout
        'network': float(Variable.get(
            "erp_error_network_probability", default_var="0.00"))  # Network
    },
    'fatal_error_probability': float(Variable.get(
        "erp_error_fatal_probability", default_var="0.00")),  # Unrecoverable
    'persistent_error_probability': float(Variable.get(
        "erp_error_persistent_probability", default_var="0.05")),  # Persistent
    'persistent_error_type': Variable.get(
        "erp_error_persistent_type", default_var="500")  # Type of persistent
}

# Track persistent failures per operation
_persistent_failures = {}


def simulate_api_error(
    operation_type: str = "unknown",
    operation_id: str = None
) -> Dict[str, Any]:
    """
    Simulates different types of API errors based on configured probabilities.

    Args:
        operation_type: Type of operation ('header', 'line', 'token')
        operation_id: Unique identifier for tracking persistent failures

    Returns:
        Dictionary with error information or None if no error simulated
    """
    if not TESTING_CONFIG['enable_error_simulation']:
        return None

    # Check for persistent errors first
    if operation_id and TESTING_CONFIG['persistent_error_probability'] > 0:
        # If this operation was already marked as persistently failing
        if operation_id in _persistent_failures:
            error_type = _persistent_failures[operation_id]['error_type']
            return _get_error_response(error_type)

        # Check if this operation should start failing persistently
        persistent_roll = random.random()
        if persistent_roll < TESTING_CONFIG['persistent_error_probability']:
            error_type = TESTING_CONFIG['persistent_error_type']
            _persistent_failures[operation_id] = {
                'error_type': error_type,
                'operation_type': operation_type
            }
            return _get_error_response(error_type)

    # Generate random number to determine if an error should occur
    error_roll = random.random()
    cumulative_probability = 0.0

    for error_type, probability in TESTING_CONFIG[
            'error_probabilities'].items():
        cumulative_probability += probability

        if error_roll < cumulative_probability:
            return _get_error_response(error_type)

    # Check for fatal errors separately (these always fail all retries)
    fatal_roll = random.random()
    if fatal_roll < TESTING_CONFIG['fatal_error_probability']:
        return {
            'status_code': 400,
            'text': ('{"error":"fatal_error",'
                     '"message":"Simulated fatal error"}'),
            'json': {
                "error": "fatal_error",
                "message": "Simulated fatal error"
            },
            'is_fatal': True  # Special flag: always fails
        }

    # If we get here, no error was simulated
    return None


def _get_error_response(error_type: str) -> Dict[str, Any]:
    """Helper function to get error response based on error type."""
    if error_type == '401':
        return {
            'status_code': 401,
            'text': ('{"error":"invalid_token",'
                     '"error_description":'
                     '"The access token expired"}'),
            'json': {
                "error": "invalid_token",
                "error_description": "The access token expired"
            }
        }
    elif error_type == '429':
        return {
            'status_code': 429,
            'text': ('{"error":"rate_limit_exceeded",'
                     '"message":"Too many requests"}'),
            'json': {
                "error": "rate_limit_exceeded",
                "message": "Too many requests"
            }
        }
    elif error_type == '500':
        return {
            'status_code': 500,
            'text': ('{"error":"internal_server_error",'
                     '"message":"Internal server error"}'),
            'json': {
                "error": "internal_server_error",
                "message": "Internal server error"
            }
        }
    elif error_type == '502':
        return {
            'status_code': 502,
            'text': '{"error":"bad_gateway","message":"Bad gateway"}',
            'json': {
                "error": "bad_gateway",
                "message": "Bad gateway"
            }
        }
    elif error_type == '503':
        return {
            'status_code': 503,
            'text': ('{"error":"service_unavailable",'
                     '"message":"Service temporarily unavailable"}'),
            'json': {
                "error": "service_unavailable",
                "message": "Service temporarily unavailable"
            }
        }
    elif error_type == '504':
        return {
            'status_code': 504,
            'text': ('{"error":"gateway_timeout",'
                     '"message":"Gateway timeout"}'),
            'json': {
                "error": "gateway_timeout",
                "message": "Gateway timeout"
            }
        }
    elif error_type == 'network':
        return {
            'network_error': True,
            'error_message': "Simulated network connection error"
        }
    else:
        return None


def make_api_request(
    method: str,
    url: str,
    headers: Dict[str, str],
    json_data: Dict[str, Any] = None,
    data: Dict[str, Any] = None,
    operation_type: str = "unknown",
    operation_id: str = None
) -> requests.Response:
    """
    Wrapper function that decides whether to use simulation or real requests.

    Args:
        method: HTTP method ('GET', 'POST', etc.)
        url: Request URL
        headers: Request headers
        json_data: JSON payload for POST requests
        data: Form data for POST requests
        operation_type: Type of operation for logging (used only in testing)
        operation_id: Unique identifier for tracking persistent failures

    Returns:
        Response object (real or simulated)
    """
    # If testing is enabled, use simulation
    if TESTING_CONFIG['enable_error_simulation']:
        return make_request_with_simulation(
            method, url, headers, json_data, data, operation_type, operation_id
        )

    # Otherwise, use normal requests (production code)
    if method.upper() == 'POST':
        if json_data is not None:
            return requests.post(url, headers=headers, json=json_data)
        elif data is not None:
            return requests.post(url, headers=headers, data=data)
        else:
            return requests.post(url, headers=headers)
    elif method.upper() == 'GET':
        return requests.get(url, headers=headers)
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")


def make_request_with_simulation(
    method: str,
    url: str,
    headers: Dict[str, str],
    json_data: Dict[str, Any] = None,
    data: Dict[str, Any] = None,
    operation_type: str = "unknown",
    operation_id: str = None
) -> requests.Response:
    """
    Makes an HTTP request with optional error simulation for testing.

    Args:
        method: HTTP method ('GET', 'POST', etc.)
        headers: Request headers
        url: Request URL
        json_data: JSON payload for POST requests
        data: Form data for POST requests
        operation_type: Type of operation for logging
        operation_id: Unique identifier for tracking persistent failures

    Returns:
        Response object (real or simulated)
    """
    # Check if we should simulate an error
    simulated_error = simulate_api_error(operation_type, operation_id)

    if simulated_error:
        if simulated_error.get('network_error'):
            # Simulate network error
            raise requests.exceptions.ConnectionError(
                simulated_error['error_message']
            )
        else:
            # Create a mock response object
            class MockResponse:
                def __init__(self, status_code: int, text: str,
                             json_data: Dict):
                    self.status_code = status_code
                    self.text = text
                    self._json_data = json_data

                def json(self):
                    return self._json_data

            return MockResponse(
                simulated_error['status_code'],
                simulated_error['text'],
                simulated_error['json']
            )

    # Make real request if no error simulation
    if method.upper() == 'POST':
        if json_data is not None:
            return requests.post(url, headers=headers, json=json_data)
        elif data is not None:
            return requests.post(url, headers=headers, data=data)
        else:
            return requests.post(url, headers=headers)
    elif method.upper() == 'GET':
        return requests.get(url, headers=headers)
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")


def get_erp_token(max_retries: int = 3) -> str:
    """
    Gets an ERP token with retry logic.
    Args:
        max_retries: Maximum number of retries (default: 3).
    Returns:
        The ERP token.
    Raises:
        Exception: If an error occurs after all retries.
    """
    erp_token_url = os.environ.get('ERP_TOKEN_URL')
    erp_url = os.environ.get('ERP_URL')
    erp_client_id = os.environ.get('ERP_CLIENT_ID')
    erp_client_secret = os.environ.get('ERP_CLIENT_SECRET')

    if not all([erp_token_url, erp_url, erp_client_id, erp_client_secret]):
        raise ValueError("Missing ERP credentials in environment variables")

    data = {
        'grant_type': 'client_credentials',
        'client_id': erp_client_id,
        'client_secret': erp_client_secret,
        'scope': f"{erp_url}/.default"
    }

    for attempt in range(max_retries):
        try:
            response = make_api_request(
                'POST',
                f"{erp_token_url}/oauth2/v2.0/token",
                {'Content-Type': 'application/x-www-form-urlencoded'},
                data=data,
                operation_type='token'
            )
        except requests.exceptions.RequestException as e:
            error_msg = f"Network error getting ERP token: {str(e)}"
            if attempt == max_retries - 1:
                raise Exception(error_msg)
            else:
                logging.warning(
                    f"Network error on attempt {attempt + 1}: {error_msg}. "
                    "Retrying...")
                continue

        if response.status_code == 200:
            token_data = response.json()
            if not token_data.get('access_token'):
                raise Exception("Error al obtener token ERP")
            return token_data['access_token']
        elif (hasattr(response, '_json_data') and
              response._json_data.get('error') == 'fatal_error'):
            # This is a simulated fatal error - should always fail
            logging.error("❌ Fatal error getting ERP token")
            raise Exception(f"Fatal error getting ERP token: {response.text}")
        elif (response.status_code in (429, 502, 503, 504) and
              attempt < max_retries - 1):
            # Rate limiting or server errors - retry with delay
            import time
            delay = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
            logging.warning(
                f"Server error {response.status_code} on "
                f"attempt {attempt + 1} getting ERP token. "
                f"Retrying in {delay} seconds...")
            time.sleep(delay)
            continue
        else:
            error_msg = (
                f"Error getting ERP token: {response.status_code} "
                f"{response.text}"
            )
            if attempt == max_retries - 1:
                # Last attempt failed, raise exception
                raise Exception(error_msg)
            else:
                logging.warning(
                    f"Attempt {attempt + 1} failed: {error_msg}. Retrying...")
                continue

    # This should never be reached, but just in case
    raise Exception(f"Failed to get ERP token after {max_retries} attempts")


def create_erp_header(
    token: str,
    receiving_warehouse_id: str,
    max_retries: int = 3
) -> str:
    """
    Creates a header in the ERP for a given transfer order with retry logic.
    Args:
        token: The ERP token.
        receiving_warehouse_id: The receiving warehouse ID.
        max_retries: Maximum number of retries (default: 3).
    Returns:
        The ERP transfer order number.
    Raises:
        Exception: If an error occurs after all retries.
    """
    erp_url = os.environ.get('ERP_URL')
    if not erp_url:
        raise ValueError("Missing ERP_URL in environment variables")

    # Format dates in ISO 8601 format with UTC timezone indicator (Z)
    from datetime import timezone
    now = datetime.now(timezone.utc)
    formatted_date = now.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Prepare the request body once
    body = {
        "dataAreaId": "pat",
        "RequestedReceiptDate": formatted_date,
        "ShippingWarehouseId": "CD",
        "ReceivingWarehouseId": receiving_warehouse_id,
        "TransferOrderPromisingMethod": "None",
        "AreLinesAutomaticallyReservedByDefault": "Yes",
        "RequestedShippingDate": formatted_date,
        "TransferOrderStockTransferPriceType": "CostPrice"
    }

    for attempt in range(max_retries):
        try:
            # Headers need to be inside loop in case token changes
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {token}'
            }

            # Create unique operation ID for persistent error tracking
            operation_id = f"header_{receiving_warehouse_id}"

            try:
                response = make_api_request(
                    'POST',
                    f"{erp_url}/data/TransferOrderHeaders",
                    headers,
                    json_data=body,
                    operation_type='header',
                    operation_id=operation_id
                )
            except requests.exceptions.RequestException as e:
                error_msg = (
                    f"Network error creating header for "
                    f"{receiving_warehouse_id}: {str(e)}"
                )
                if attempt == max_retries - 1:
                    raise Exception(error_msg)
                else:
                    logging.warning(
                        f"Network error on attempt {attempt + 1}: "
                        f"{error_msg}. Retrying...")
                    continue

            if response.status_code in (200, 201):
                data = response.json()
                transfer_order_number = data.get('TransferOrderNumber')
                logging.info(
                    f"✅ Created ERP header for store "
                    f"{receiving_warehouse_id}: {transfer_order_number}")
                return transfer_order_number
            elif (hasattr(response, '_json_data') and
                  response._json_data.get('error') == 'fatal_error'):
                # This is a simulated fatal error - should always fail
                logging.error(
                    f"❌ Fatal error for header {receiving_warehouse_id} - "
                    "omitting this store")
                raise Exception(
                    f"Fatal error for warehouse {receiving_warehouse_id}: "
                    f"{response.text}")
            elif response.status_code == 401 and attempt < max_retries - 1:
                # Token expired, get new token and retry
                logging.warning(
                    f"Token expired (401) on attempt {attempt + 1} for "
                    f"warehouse {receiving_warehouse_id}. "
                    "Getting new token...")
                token = get_erp_token()
                continue
            elif (response.status_code in (429, 502, 503, 504) and
                  attempt < max_retries - 1):
                # Rate limiting or server errors - retry with delay
                import time
                delay = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                logging.warning(
                    f"Server error {response.status_code} on "
                    f"attempt {attempt + 1} for warehouse "
                    f"{receiving_warehouse_id}. "
                    f"Retrying in {delay} seconds...")
                time.sleep(delay)
                continue
            else:
                error_text = response.text
                error_msg = (
                    f"Error al crear cabecera para {receiving_warehouse_id}: "
                    f"{response.status_code} {error_text}"
                )
                if attempt == max_retries - 1:
                    # Last attempt failed, raise exception
                    raise Exception(error_msg)
                else:
                    logging.warning(
                        f"Attempt {attempt + 1} failed: {error_msg}. "
                        "Retrying...")
                    continue

        except requests.exceptions.RequestException as e:
            error_msg = (
                f"Network error creating header for {receiving_warehouse_id}: "
                f"{str(e)}"
            )
            if attempt == max_retries - 1:
                raise Exception(error_msg)
            else:
                logging.warning(
                    f"Network error on attempt {attempt + 1}: {error_msg}. "
                    "Retrying...")
                continue

    # This should never be reached, but just in case
    raise Exception(
        f"Failed to create header after {max_retries} attempts for "
        f"warehouse {receiving_warehouse_id}"
    )


def create_erp_line(
    token: str,
    transfer_order_number: str,
    line_data: Dict[str, Any],
    max_retries: int = 3
) -> Dict[str, str]:
    """
    Creates a line in the ERP for a given transfer order with retry logic.
    Args:
        token: The ERP token.
        transfer_order_number: The transfer order number.
        line_data: The line data.
        max_retries: Maximum number of retries (default: 3).
    Returns:
        Dictionary with the ERP line ID.
    Raises:
        Exception: If an error occurs after all retries.
    """
    erp_url = os.environ.get('ERP_URL')
    if not erp_url:
        raise ValueError("Missing ERP_URL in environment variables")

    # Prepare the request body once - it doesn't change between retries
    body = {
        "dataAreaId": "pat",
        "TransferOrderNumber": transfer_order_number,
        "LineNumber": line_data['LineNumber'],
        "OrderedInventoryStatusId":
            line_data['OrderedInventoryStatusId'],
        "ProductStyleId": line_data['ProductStyleId'],
        "TransferQuantity": line_data['TransferQuantity'],
        "RequestedReceiptDate": line_data['RequestedReceiptDate'],
        "RequestedShippingDate": line_data['RequestedShippingDate'],
        "ProductConfigurationId": line_data['ProductConfigurationId'],
        "ProductSizeId": line_data['ProductSizeId'],
        "ProductColorId": line_data['ProductColorId'],
        "ItemNumber": line_data['ItemNumber'],
        "ShippingWarehouseLocationId":
        line_data['ShippingWarehouseLocationId'],
        "SalesTaxItemGroupCodeShipment":
            line_data['SalesTaxItemGroupCodeShipment'],
        "SalesTaxItemGroupCodeReceipt":
            line_data['SalesTaxItemGroupCodeReceipt'],
        "PriceType": line_data['PriceType'],
        # Fixed fields
        "ATPTimeFenceDays": 0,
        "AllowedUnderdeliveryPercentage": 100,
        "WillProductReceivingCrossDockProducts": "No",
        "OverrideFEFODateControl": "No",
        "IntrastatCostAmount": 0,
        "ATPDelayedSupplyOffsetDays": 0,
        "IntrastatStatisticalValue": 0,
        "OverrideSalesTaxShipment": "No",
        "TransferCatchWeightQuantity": 0,
        "PlanningPriority": 0,
        "OverrideSalesTaxReceipt": "No",
        "TransferOrderPromisingMethod": "None",
        "AllowedOverdeliveryPercentage": 0,
        "ATPBackwardSupplyTimeFenceDays": 0,
        "IsAutomaticallyReserved": "Yes",
        "IsATPIncludingPlannedOrders": False,
        "ATPDelayedDemandOffsetDays": 0,
        "InventCostPriceCalculated": 0,
        "MaximumRetailPrice": 0,
        "NetAmount": 0,
        "DefaultDimension": 0,
        "UnitPrice": 0,
        "CurrencyCode": "",
        "AssessableValueTransactionCurrency": 0,
        "InvntCostPrice": 0,
        "Retention": 0,
        "VATPriceType": "CostPrice"
    }

    for attempt in range(max_retries):
        try:
            # Headers need to be inside the loop because token might change
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {token}'
            }

            # Create unique operation ID for persistent error tracking
            operation_id = (f"line_{transfer_order_number}_"
                            f"{line_data['LineNumber']}")

            try:
                response = make_api_request(
                    'POST',
                    f"{erp_url}/data/TransferOrderLines",
                    headers,
                    json_data=body,
                    operation_type='line',
                    operation_id=operation_id
                )
            except requests.exceptions.RequestException as e:
                error_msg = (
                    f"Network error creating line for TR "
                    f"{transfer_order_number}, line "
                    f"{line_data['LineNumber']}: {str(e)}"
                )
                if attempt == max_retries - 1:
                    raise Exception(error_msg)
                else:
                    logging.warning(
                        f"Network error on attempt {attempt + 1}: "
                        f"{error_msg}. Retrying...")
                    continue

            if response.status_code in (200, 201):
                data = response.json()
                line_id = data.get("ShippingInventoryLotId")
                logging.info(
                    f"✅ Created ERP line for TR {transfer_order_number}, "
                    f"line {line_data['LineNumber']}: {line_id}")
                return {"ERP_LINE_ID": line_id}
            elif (hasattr(response, '_json_data') and
                  response._json_data.get('error') == 'fatal_error'):
                # This is a simulated fatal error - should always fail
                logging.error(
                    f"❌ Fatal error for line TR {transfer_order_number}, "
                    f"line {line_data['LineNumber']} - omitting this line")
                raise Exception(
                    f"Fatal error for TR {transfer_order_number}, "
                    f"line {line_data['LineNumber']}: {response.text}")
            elif response.status_code == 401 and attempt < max_retries - 1:
                # Token expired, get new token and retry
                logging.warning(
                    f"Token expired (401) on attempt {attempt + 1} for "
                    f"TR {transfer_order_number}, "
                    f"line {line_data['LineNumber']}. "
                    "Getting new token...")
                token = get_erp_token()
                continue
            elif (response.status_code in (429, 502, 503, 504) and
                  attempt < max_retries - 1):
                # Rate limiting or server errors - retry with backoff
                import time
                delay = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                logging.warning(
                    f"Server error {response.status_code} on "
                    f"attempt {attempt + 1} for TR {transfer_order_number}, "
                    f"line {line_data['LineNumber']}. "
                    f"Retrying in {delay} seconds...")
                time.sleep(delay)
                continue
            elif response.status_code == 400 and attempt < max_retries - 1:
                # Bad request - could be temporary data issue, retry once
                logging.warning(
                    f"Bad request (400) on attempt {attempt + 1} for "
                    f"TR {transfer_order_number}, "
                    f"line {line_data['LineNumber']}. "
                    f"Response: {response.text}. Retrying...")
                continue
            else:
                error_text = response.text
                error_msg = (
                    f"Error al crear línea para TR {transfer_order_number}: "
                    f"{response.status_code} {error_text}"
                )
                if attempt == max_retries - 1:
                    # Last attempt failed, raise exception
                    raise Exception(error_msg)
                else:
                    logging.warning(
                        f"Attempt {attempt + 1} failed: {error_msg}. "
                        "Retrying...")
                    continue

        except requests.exceptions.RequestException as e:
            error_msg = (
                f"Network error creating line for TR {transfer_order_number}: "
                f"{str(e)}"
            )
            if attempt == max_retries - 1:
                raise Exception(error_msg)
            else:
                logging.warning(
                    f"Network error on attempt {attempt + 1}: {error_msg}. "
                    "Retrying...")
                continue

    # This should never be reached, but just in case
    raise Exception(
        f"Failed to create line after {max_retries} attempts for "
        f"TR {transfer_order_number}"
    )


def get_replenishment_data(replenishment_id: str) -> Dict[str, Any]:
    """
    Get replenishment header data from Snowflake.
    Args:
        replenishment_id: The replenishment ID.
    Returns:
        Dict with replenishment header data.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    snowflake_conn_id = default_args['snowflake_conn_id']
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    query = f"""
    SELECT * FROM PATAGONIA.CORE_TEST.PATCORE_REPLENISHMENTS
    WHERE ID = '{replenishment_id}'
    """

    result = snowflake_hook.get_pandas_df(query)

    if result.empty:
        raise ValueError(f"No replenishment found with ID: {replenishment_id}")
    return result.iloc[0].to_dict()


def get_enriched_lines(replenishment_id: str) -> pd.DataFrame:
    """
    Get enriched replenishment line data from Snowflake.
    Args:
        replenishment_id: The replenishment ID.
    Returns:
        DataFrame with enriched line data.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    snowflake_conn_id = default_args['snowflake_conn_id']
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    # Query with a fallback for SKUs not found in ERP_PRODUCTS
    query = f"""
    SELECT
        ROW_NUMBER()
            OVER (PARTITION BY rpl.STORE ORDER BY rpl.SKU) AS LINENUMBER,
        prod.ITEMNUMBER,
        'DISPONIBLE' AS ORDEREDINVENTORYSTATUSID,
        prod.COLOR AS PRODUCTCOLORID,
        prod.CONFIGURATION AS PRODUCTCONFIGURATIONID,
        prod.SIZE AS PRODUCTSIZEID,
        'GEN' AS PRODUCTSTYLEID,
        'CD' AS SHIPPINGWAREHOUSEID,
        'GENERICA' AS SHIPPINGWAREHOUSELOCATIONID,
        rpl.REPLENISHMENT AS TRANSFERQUANTITY,
        rpl.STORE AS TIENDA,
        rpl.SKU,
        prod.TEAM,
        prod.CATEGORY,
        prod.PRODUCTNAME,
        rpl.DELIVERY,
        rpl.ERP_TR_ID,
        rpl.ERP_LINE_ID,
        CASE WHEN prod.ITEMNUMBER IS NULL THEN TRUE
             ELSE FALSE END AS IS_MISSING_ERP_DATA
    FROM PATAGONIA.CORE_TEST.PATCORE_REPLENISHMENTS_LINE rpl
    LEFT JOIN PATAGONIA.CORE_TEST.ERP_PRODUCTS prod
        ON rpl.SKU = prod.SKU
    WHERE rpl.REPLENISHMENT_ID = '{replenishment_id}'
    ORDER BY rpl.STORE, LINENUMBER
    """

    result = snowflake_hook.get_pandas_df(query)
    return result


def update_erp_info_in_replenishment(
    replenishment_id: str,
    erp_trs: str,
    lines: List[Dict[str, str]],
    wms_status_by_tr: Dict[str, Dict[str, Any]] = None
):
    """
    Update ERP info and WMS status in replenishment records using a temporary
    table for efficiency.
    Args:
        replenishment_id: The replenishment ID.
        erp_trs: Comma-separated list of ERP transfer IDs.
        lines: List of line info with ERP IDs.
        wms_status_by_tr: Dict with TR_ID as key and WMS status as value.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    import uuid

    snowflake_conn_id = default_args['snowflake_conn_id']
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    # Update the header with the ERP TR IDs
    header_query = f"""
    UPDATE PATAGONIA.CORE_TEST.PATCORE_REPLENISHMENTS
    SET ERP_TRS_IDS = '{erp_trs}'
    WHERE ID = '{replenishment_id}'
    """

    snowflake_hook.run(header_query)

    # If no lines to update, just return
    if not lines:
        logging.info("No lines to update in replenishment")
        return

    # Create a unique temporary table name to avoid conflicts
    temp_table_name = f"TEMP_REPLENISHMENT_UPDATE_{uuid.uuid4().hex[:8]}"

    # Use a single connection for all operations
    with snowflake_hook.get_conn() as conn:
        with conn.cursor() as cur:
            # Create temporary table
            create_temp_table_query = f"""
            CREATE TEMPORARY TABLE PATAGONIA.CORE_TEST.{temp_table_name} (
                ERP_TR_ID STRING,
                ERP_LINE_ID STRING,
                REPLENISHMENT_ID STRING,
                SKU STRING,
                STORE STRING
            )
            """
            cur.execute(create_temp_table_query)

            # Insert data into temporary table -
            # do it in batches to avoid query size limits
            batch_size = 1000
            for i in range(0, len(lines), batch_size):
                batch = lines[i:i+batch_size]

                # Build the values part of the INSERT statement
                values_clauses = []
                for line in batch:
                    values_clauses.append(
                        f"('{line['ERP_TR_ID']}', "
                        f"'{line['ERP_LINE_ID']}', "
                        f"'{replenishment_id}', "
                        f"'{line['SKU']}', "
                        f"'{line['STORE']}')"
                    )

                values_str = ",".join(values_clauses)

                insert_query = f"""
                INSERT INTO PATAGONIA.CORE_TEST.{temp_table_name} (
                    ERP_TR_ID, ERP_LINE_ID, REPLENISHMENT_ID, SKU, STORE
                )
                VALUES
                {values_str}
                """
                cur.execute(insert_query)

            # Update replenishment lines with temporary data
            update_query = f"""
            UPDATE PATAGONIA.CORE_TEST.PATCORE_REPLENISHMENTS_LINE l
            SET
                l.ERP_TR_ID = t.ERP_TR_ID,
                l.ERP_LINE_ID = t.ERP_LINE_ID
            FROM PATAGONIA.CORE_TEST.{temp_table_name} t
            WHERE
                l.REPLENISHMENT_ID = t.REPLENISHMENT_ID
                AND l.SKU = t.SKU
                AND l.STORE = t.STORE
            """
            cur.execute(update_query)

            # Drop temporary table
            drop_query = f"DROP TABLE PATAGONIA.CORE_TEST.{temp_table_name}"
            cur.execute(drop_query)

            # Commit the transaction
            conn.commit()

    logging.info("Updated ERP info in replenishment "
                 "successfully with batch update")

    # Update WMS status if provided
    if wms_status_by_tr:
        for tr_id, status in wms_status_by_tr.items():
            wms_status = status.get('message', 'Unknown')

            update_query = f"""
            UPDATE PATAGONIA.CORE_TEST.PATCORE_REPLENISHMENTS_LINE
            SET WMS_SEND_STATUS = '{wms_status}'
            WHERE REPLENISHMENT_ID = '{replenishment_id}'
            AND ERP_TR_ID = '{tr_id}'
            """

            try:
                snowflake_hook.run(update_query)
                logging.info(
                    f"Updated WMS status for TR {tr_id}: {wms_status}")
            except Exception as e:
                logging.error(f"Error updating WMS status for TR {tr_id}: {e}")


def build_wms_xml_document(
    tr_id: str,
    warehouse: int,
    store: str,
    fecha: str,
    lines: List[Dict[str, Any]]
) -> str:
    """
    Construye el XML SOAP para RegistrarDocumentoSalida de un TR.
    `lines` debe contener al menos: SKU, CANTIDAD, NRO_LINEA.
    Usa STORE_DELIVERY_INFO para completar campos de dirección según la tienda.
    - IdOwner siempre es PATAGONIA (WMS_OWNER).
    - IdAlmacen e IdAlmacenDestino usan el mismo valor (parametrizable arriba).
    - PaisDespacho se deja fijo como CHL.
    - Observaciones = "{direccion}, {ciudad}".
    """
    # Get dispatch info for the store;
    # if it doesn't exist, use simple defaults
    info = STORE_DELIVERY_INFO.get(store, {
        "region": "",
        "comuna": "",
        "ciudad": "",
        "direccion": "",
    })

    direccion = info["direccion"]
    ciudad = info["ciudad"]
    observaciones = f"{direccion}, {ciudad}" if direccion or ciudad else ""

    detalles_xml = ""
    for line in lines:
        detalles_xml += f"""
    <DetalleSalida>
      <IdArticulo>{line['SKU']}</IdArticulo>
      <NroLinea>{line['NRO_LINEA']}</NroLinea>
      <NroLote></NroLote>
      <Cantidad>{int(line['CANTIDAD'])}</Cantidad>
    </DetalleSalida>"""

    logging.info(f"Construyendo XML WMS para TR {tr_id}...")

    xml = f"""<soap:Envelope \
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
xmlns:xsd="http://www.w3.org/2001/XMLSchema" \
xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
<soap:Body>
<RegistrarDocumentoSalida xmlns="http://www.stgchile.cl/WMSTek/EIT/Interfaces">
<documento>
  <IdAlmacen>{warehouse}</IdAlmacen>
  <IdOwner>{WMS_OWNER}</IdOwner>
  <IdDocSalida>{tr_id}</IdDocSalida>
  <NroReferencia>{tr_id}</NroReferencia>
  <NroOrdenCliente></NroOrdenCliente>
  <Tipo>TRF</Tipo>
  <FechaEmision>{fecha}</FechaEmision>
  <FechaCompromiso>{fecha}</FechaCompromiso>
  <FechaExpiracion>{fecha}</FechaExpiracion>
  <IdAlmacenDestino>{warehouse}</IdAlmacenDestino>
  <IdCliente>76018478-0</IdCliente>
  <CodCliente>76018478-0</CodCliente>
  <NomCliente>Patagonia Chile Limitada</NomCliente>
  <GiroCliente></GiroCliente>
  <DireccionCliente />
  <ComunaCliente />
  <CiudadCliente />
  <LocalidadCliente />
  <PaisCliente />
  <DireccionDespacho>{direccion}</DireccionDespacho>
  <ComunaDespacho>{info['comuna']}</ComunaDespacho>
  <CiudadDespacho>{ciudad}</CiudadDespacho>
  <LocalidadDespacho>{info['region']}</LocalidadDespacho>
  <PaisDespacho>CHL</PaisDespacho>
  <Prioridad>0</Prioridad>
  <Observaciones>{observaciones}</Observaciones>
  <IdVendedor />
  <NomVendedor></NomVendedor>
  <IdSucursal>{store}</IdSucursal>
  <FormadePago />
  <TipoDespacho></TipoDespacho>
  <CobroTransporte>NOR</CobroTransporte>
  <ImpFactura>N</ImpFactura>
  <ImpGuia>N</ImpGuia>
  <ImpBoleta>N</ImpBoleta>
  <EstadoInterfaz>C</EstadoInterfaz>
  <Detalles>{detalles_xml}
  </Detalles>
</documento>
</RegistrarDocumentoSalida>
</soap:Body>
</soap:Envelope>"""
    return xml


def send_single_tr_to_wms(
    tr_id: str,
    store: str,
    successful_lines_for_tr: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Envía un TR específico al WMS, usando sus líneas creadas
    exitosamente en el ERP.
    successful_lines_for_tr debe contener dicts con:
      - SKU
      - STORE
      - CANTIDAD
      - NRO_LINEA

    Returns:
        Dict con 'success' (bool), 'message' (str),
        y 'status_code' (int opcional)
    """
    import requests
    from requests.auth import HTTPBasicAuth
    from datetime import datetime

    # WMS Settings from environment variables
    wms_url = os.environ.get('WMS_TEK2_URL')
    wms_username = os.environ.get('WMS_TEK2_USER')
    wms_password = os.environ.get('WMS_TEK2_PASS')

    if not all([wms_url, wms_username, wms_password]):
        raise ValueError("Missing WMS credentials in environment variables")

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": (
            '"http://www.stgchile.cl/WMSTek/EIT/Interfaces/'
            'RegistrarDocumentoSalida"'
        ),
    }

    fecha = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    xml_body = build_wms_xml_document(
        tr_id=tr_id,
        warehouse=ID_WAREHOUSE_WMS,
        store=store,
        fecha=fecha,
        lines=successful_lines_for_tr,
    )

    # Log the complete XML that will be sent
    logging.info(
        f"[WMS] XML completo a enviar para TR {tr_id} (store {store}):"
    )
    logging.info(f"\n{xml_body}")
    logging.info(f"[WMS] Fin del XML para TR {tr_id}")

    logging.info(f"Enviando TR {tr_id} (store {store}) al WMS...")
    resp = requests.post(
        wms_url,
        headers=headers,
        data=xml_body.encode("utf-8"),
        auth=HTTPBasicAuth(wms_username, wms_password),
        timeout=30
    )

    if resp.status_code != 200:
        error_msg = f"Error HTTP {resp.status_code}: {resp.text[:200]}"
        logging.error(
            f"Error HTTP enviando TR {tr_id} al WMS: "
            f"{resp.status_code} {resp.text[:500]}"
        )
        return {
            'success': False,
            'message': error_msg,
            'status_code': resp.status_code
        }
    else:
        logging.info(
            f"Respuesta WMS para TR {tr_id}: {resp.text[:500]}"
        )
        return {
            'success': True,
            'message': 'Enviado exitosamente al WMS',
            'status_code': 200
        }


def send_replenishment_summary_email(
    replenishment_id: str,
    erp_tr_numbers: List[str],
    lines_created: int,
    missing_skus: List[Dict[str, str]],
    user: str
):
    """
    Send a summary email about the replenishment process.
    Args:
        replenishment_id: The replenishment ID.
        erp_tr_numbers: List of ERP transfer order numbers.
        lines_created: Number of lines created.
        missing_skus: List of SKUs that couldn't be found in ERP.
        user: User who created the replenishment.
    Returns:
        A dictionary with subject and html_content for the email.
    """
    # Get ERP URL from environment
    erp_url = os.environ.get('ERP_URL', '')
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    snowflake_conn_id = default_args['snowflake_conn_id']
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    # Get header info
    query = f"""
    SELECT * FROM PATAGONIA.CORE_TEST.PATCORE_REPLENISHMENTS
    WHERE ID = '{replenishment_id}'
    """
    header = snowflake_hook.get_pandas_df(query)

    # Get summary by ERP TR
    query = f"""
    SELECT
        ERP_TR_ID, STORE, COUNT(*) as LINE_COUNT,
        SUM(REPLENISHMENT) as TOTAL_UNITS,
        MAX(WMS_SEND_STATUS) as WMS_STATUS
    FROM PATAGONIA.CORE_TEST.PATCORE_REPLENISHMENTS_LINE
    WHERE REPLENISHMENT_ID = '{replenishment_id}'
    AND ERP_TR_ID IS NOT NULL
    GROUP BY ERP_TR_ID, STORE
    ORDER BY STORE
    """
    summary = snowflake_hook.get_pandas_df(query)

    chile_tz = pytz.timezone('America/Santiago')
    current_time = datetime.now(chile_tz)
    created_date = current_time.strftime('%d-%m-%Y %H:%M')
    week = f'W{current_time.isocalendar()[1]}'

    erp_tr_list = ', '.join(erp_tr_numbers) if erp_tr_numbers else 'None'

    # Safely access header values - check if header is not empty first
    if not header.empty:
        stores_considered = (
            header['STORES_CONSIDERED'].values[0]
            if 'STORES_CONSIDERED' in header.columns else 'N/A'
        )
        total_units = (
            header['TOTAL_REPLENISHMENT'].values[0]
            if 'TOTAL_REPLENISHMENT' in header.columns else 'N/A'
        )
        selected_deliveries = (
            header['SELECTED_DELIVERIES'].values[0]
            if 'SELECTED_DELIVERIES' in header.columns else 'N/A'
        )
    else:
        stores_considered = 'N/A'
        total_units = 'N/A'
        selected_deliveries = 'N/A'

    # Prepare summary table
    summary_table = ""
    if not summary.empty:
        summary_table = "<table border='1' cellpadding='5' cellspacing='0'>"
        header_row = (
            "<tr><th>Store</th><th>ERP Transfer</th>"
            "<th>Line Count</th><th>Total Units</th>"
            "<th>WMS Status</th></tr>"
        )
        summary_table += header_row

        for _, row in summary.iterrows():
            wms_status = row.get('WMS_STATUS', 'N/A') or 'N/A'

            summary_table += "<tr>"
            summary_table += f"<td>{row['STORE']}</td>"
            summary_table += f"<td>{row['ERP_TR_ID']}</td>"
            summary_table += f"<td>{row['LINE_COUNT']}</td>"
            summary_table += f"<td>{row['TOTAL_UNITS']}</td>"
            summary_table += f"<td>{wms_status}</td>"
            summary_table += "</tr>"

        summary_table += "</table>"
    else:
        summary_table = "<p>No lines were created in ERP</p>"

    # Prepare missing SKUs table
    missing_skus_table = ""

    if missing_skus:
        missing_skus_table = (
            "<table border='1' cellpadding='5' cellspacing='0'>"
        )
        missing_skus_table += (
            "<tr><th>Store</th><th>SKU</th>"
            "<th>Quantity</th><th>Error</th><th>TR Number</th></tr>"
        )

        for sku in missing_skus:
            missing_skus_table += "<tr>"
            missing_skus_table += f"<td>{sku.get('STORE', 'N/A')}</td>"
            missing_skus_table += f"<td>{sku.get('SKU', 'N/A')}</td>"
            missing_skus_table += f"<td>{sku.get('QUANTITY', 0)}</td>"
            missing_skus_table += f"<td>{sku.get('ERROR', 'Unknown')}</td>"
            missing_skus_table += f"<td>{sku.get('TR_NUMBER', 'N/A')}</td>"
            missing_skus_table += "</tr>"

        missing_skus_table += "</table>"
    else:
        missing_skus_table = "<p>No failed items</p>"

    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            .header {{ background-color: #f2f2f2; padding: 20px; }}
            .content {{ padding: 20px; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th {{ background-color: #f2f2f2; text-align: left; }}
            td, th {{ padding: 8px; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h2>Replenishment Summary: {replenishment_id}</h2>
            <p><strong>Created Date:</strong> {created_date} hrs</p>
            <p><strong>Created by:</strong> {user}</p>
            <p><strong>Week:</strong> {week}</p>
            <p><strong>Stores:</strong> {stores_considered}</p>
            <p><strong>Selected Deliveries:</strong> {selected_deliveries}</p>
            <p><strong>Total units:</strong> {total_units}</p>
            <p><strong>ERP Transfers Created:</strong> {erp_tr_list}</p>
            <p><strong>Lines Created:</strong> {lines_created}</p>
            <p><a href="{erp_url}/?cmp=PAT&mi=InventTransferOrder"
                style="background-color: #35446f;
                    color: white;
                    padding: 8px 15px;
                    text-decoration: none;
                    border-radius: 4px;
                    display: inline-block;
                    margin-top: 10px;">Ver en ERP</a></p>
        </div>
        <div class="content">
            <h3>Transfer Orders Summary</h3>
            {summary_table}

            <h3>Failed Items</h3>
            {missing_skus_table}
        </div>
    </body>
    </html>
    """

    return {
        'subject': (
            f'Replenishment {replenishment_id} - '
            'ERP Transfer Orders Created'
        ),
        'html_content': html_content
    }


def process_replenishment(**context):
    """
    Main function to process a replenishment and create it in ERP.
    """
    # Show testing configuration if enabled
    if TESTING_CONFIG['enable_error_simulation']:
        logging.info("🧪 [TESTING] Error simulation ENABLED")
        error_probs = TESTING_CONFIG['error_probabilities']
        error_count = sum(1 for p in error_probs.values() if p > 0)
        fatal_prob = TESTING_CONFIG['fatal_error_probability']
        if error_count > 0 or fatal_prob > 0:
            fatal_msg = ', fatal errors enabled' if fatal_prob > 0 else ''
            logging.info(f"🧪 [TESTING] {error_count} error types "
                         f"configured{fatal_msg}")
    else:
        logging.info("✅ [PRODUCTION] Error simulation DISABLED")

    # Get replenishment ID from DAG run configuration
    dag_run = context['dag_run']
    replenishment_id = dag_run.conf.get('replenishmentID')
    user = dag_run.conf.get('user')
    send_to_wms = dag_run.conf.get('sendToWMS', False)

    if not replenishment_id:
        raise ValueError(
            "replenishmentID not provided in DAG run configuration")

    logging.info(f"Processing replenishment ID: {replenishment_id}")

    # Get replenishment data from Snowflake
    replenishment_data = get_replenishment_data(replenishment_id)
    logging.info(
        f"Retrieved replenishment header data: {replenishment_data}")

    # Get enriched line data
    enriched_lines = get_enriched_lines(replenishment_id)
    logging.info(f"Retrieved {len(enriched_lines)} enriched lines")

    # Check for missing ERP data
    missing_erp_data = enriched_lines[enriched_lines['IS_MISSING_ERP_DATA']]
    if not missing_erp_data.empty:
        missing_skus = missing_erp_data['SKU'].tolist()
        logging.warning(
            f"Found {len(missing_skus)} SKUs without ERP data: {missing_skus}")

    # Filter out lines with missing ERP data
    valid_lines = enriched_lines[~enriched_lines['IS_MISSING_ERP_DATA']]

    if valid_lines.empty:
        raise ValueError(
            "No valid lines with ERP data found, "
            "cannot create ERP replenishments")

    # Group lines by store
    lines_by_store = {}
    for _, line in valid_lines.iterrows():
        store = line['TIENDA']
        if store not in lines_by_store:
            lines_by_store[store] = []
        lines_by_store[store].append(line.to_dict())

    # Get ERP token
    token = get_erp_token()

    # Process each store
    erp_tr_numbers = []
    erp_lines_with_info = []
    failed_lines = []  # Track failed line creations
    wms_status_by_tr = {}  # Track WMS send status per TR

    for store, store_lines in lines_by_store.items():
        logging.info(f"Creating ERP header for store: {store}")
        try:
            # Create TransferHeader for this store
            transfer_order_number = create_erp_header(token, store)

            erp_tr_numbers.append(transfer_order_number)

            # Format dates for ERP API - ISO 8601 with UTC timezone
            from datetime import timezone
            now = datetime.now(timezone.utc)
            formatted_date = now.strftime('%Y-%m-%dT%H:%M:%SZ')

            successful_lines_for_tr = []

            # Create lines for this Transfer Order
            for i, line in enumerate(store_lines):
                line_data = {
                    "ItemNumber": line['ITEMNUMBER'],
                    "ProductColorId": line['PRODUCTCOLORID'],
                    "ProductConfigurationId": line['PRODUCTCONFIGURATIONID'],
                    "ProductSizeId": line['PRODUCTSIZEID'],
                    "ProductStyleId": line['PRODUCTSTYLEID'],
                    "OrderedInventoryStatusId":
                        line['ORDEREDINVENTORYSTATUSID'],
                    "ShippingWarehouseLocationId":
                        line['SHIPPINGWAREHOUSELOCATIONID'],
                    "TransferQuantity": line['TRANSFERQUANTITY'],
                    "RequestedReceiptDate": formatted_date,
                    "RequestedShippingDate": formatted_date,
                    "SalesTaxItemGroupCodeShipment": "IVA",
                    "SalesTaxItemGroupCodeReceipt": "EXENTO",
                    "PriceType": "CostPrice",
                    "LineNumber": i + 1
                }

                try:
                    line_result = create_erp_line(
                        token, transfer_order_number, line_data)

                    erp_lines_with_info.append({
                        "SKU": line['SKU'],
                        "STORE": line['TIENDA'],
                        "ERP_TR_ID": transfer_order_number,
                        "ERP_LINE_ID": line_result["ERP_LINE_ID"]
                    })

                    successful_lines_for_tr.append({
                        "SKU": line['SKU'],
                        "STORE": line['TIENDA'],
                        "CANTIDAD": line['TRANSFERQUANTITY'],
                        "NRO_LINEA": i + 1
                    })

                except Exception as line_error:
                    # Log the error but continue processing other lines
                    error_msg = str(line_error)
                    logging.error(
                        f"Failed to create line {i+1} for "
                        f"{transfer_order_number} (SKU: {line['SKU']}): "
                        f"{error_msg}")

                    # Add to failed lines list
                    failed_lines.append({
                        'STORE': line['TIENDA'],
                        'SKU': line['SKU'],
                        'QUANTITY': line['TRANSFERQUANTITY'],
                        'ERROR': f"ERP Line Creation Error: {error_msg}",
                        'TR_NUMBER': transfer_order_number,
                        'LINE_NUMBER': i + 1
                    })

                    # Continue with next line
                    continue

            # Send to WMS if enabled
            if send_to_wms and successful_lines_for_tr:
                try:
                    wms_result = send_single_tr_to_wms(
                        transfer_order_number,
                        store,
                        successful_lines_for_tr
                    )
                    wms_status_by_tr[transfer_order_number] = wms_result
                except Exception as wms_error:
                    error_msg = f"Exception: {str(wms_error)[:200]}"
                    logging.error(
                        f"Error enviando TR {transfer_order_number} "
                        f"de tienda {store} al WMS: {wms_error}"
                    )
                    wms_status_by_tr[transfer_order_number] = {
                        'success': False,
                        'message': error_msg
                    }
            elif not send_to_wms:
                # Mark that sending to WMS was disabled
                wms_status_by_tr[transfer_order_number] = {
                    'success': None,
                    'message': 'Envío a WMS desactivado'
                }

        except Exception as e:
            logging.error(f"Error processing store {store}: {str(e)}")
            # Add all lines from this store to failed lines
            for i, line in enumerate(store_lines):
                failed_lines.append({
                    'STORE': line['TIENDA'],
                    'SKU': line['SKU'],
                    'QUANTITY': line['TRANSFERQUANTITY'],
                    'ERROR': f"Store Processing Error: {str(e)}",
                    'TR_NUMBER': 'N/A',
                    'LINE_NUMBER': i + 1
                })
            # Continue with next store instead of raising
            continue

    # Update ERP info and WMS status in Snowflake
    if erp_tr_numbers and erp_lines_with_info:
        erp_trs_str = ', '.join(erp_tr_numbers)
        logging.info(
            f"Updating replenishment with ERP TRs: {erp_trs_str}")
        update_erp_info_in_replenishment(
            replenishment_id, erp_trs_str, erp_lines_with_info,
            wms_status_by_tr
        )
        logging.info(
            "Updated ERP info and WMS status in replenishment successfully")

    # Log summary of results
    total_attempted = len(valid_lines)
    total_successful = len(erp_lines_with_info)
    total_failed = len(failed_lines) + len(missing_erp_data)

    logging.info("Processing completed:")
    logging.info(f"  - Successfully created: {total_successful} ERP lines")
    if total_failed > 0:
        logging.info(f"  - Failed to create: {total_failed} lines")
        if len(failed_lines) > 0:
            logging.info(f"    - API failures: {len(failed_lines)}")
        if len(missing_erp_data) > 0:
            logging.info(f"    - Missing ERP data: {len(missing_erp_data)}")

    # Prepare and send email summary
    summary = {
        "replenishment_id": replenishment_id,
        "erp_tr_numbers": erp_tr_numbers,
        "lines_created": len(erp_lines_with_info),
        "missing_skus":
            len(missing_erp_data) if not missing_erp_data.empty else 0,
        "failed_lines": len(failed_lines),
        "total_failed": total_failed
    }

    # Prepare and send email summary
    # Convert missing_erp_data to list of dicts for email
    missing_skus_data = []
    if not missing_erp_data.empty:
        missing_skus_data = [
            {
                'STORE': row['TIENDA'],
                'SKU': row['SKU'],
                'QUANTITY': row['TRANSFERQUANTITY'],
                'ERROR': 'Missing ERP data'
            }
            for _, row in missing_erp_data.iterrows()
        ]

    # Combine missing SKUs with failed lines for email
    all_failed_items = missing_skus_data + failed_lines

    email_data = send_replenishment_summary_email(
        replenishment_id, erp_tr_numbers,
        len(erp_lines_with_info),
        all_failed_items,
        user
    )

    # Send email
    from airflow.operators.email import EmailOperator

    # Get email recipients from Airflow Variable, config, or context
    email_recipients_str = Variable.get(
        'erp_replenishments_emails',
        default_var=''  # Empty default to fall back to config
    )

    # If Variable is set, use it (comma-separated emails)
    if email_recipients_str.strip():
        recipients = \
            [email.strip() for email in email_recipients_str.split(',')]
    else:
        # Otherwise fallback to config and context as before
        recipients = (
            dag_config.get('notification_emails') or
            [context['dag_run'].conf.get('email')] or
            [default_args['email']]
        )

    # Remove any None or empty values
    recipients = [email for email in recipients if email]

    if recipients:
        email = EmailOperator(
            task_id='send_email_summary',
            to=recipients,
            subject=email_data['subject'],
            html_content=email_data['html_content'],
            dag=context['dag']
        )
        email.execute(context=context)
        logging.info(f"Email summary sent to: {', '.join(recipients)}")
    else:
        logging.warning("No email recipients configured, "
                        "skipping email notification")

    # Decide if the DAG should be considered successful or failed
    if total_successful == 0:
        # If no lines were created successfully, fail the DAG
        raise Exception(
            f"Failed to create any ERP lines. "
            f"Total attempted: {total_attempted}, "
            f"Total failed: {total_failed}. "
            f"Check the logs and email summary for details."
        )
    elif total_failed > 0:
        # If some lines failed but some succeeded, log warning but don't fail
        logging.warning(
            f"Partial success: {total_successful} lines created successfully, "
            f"but {total_failed} lines failed. "
            f"Check email summary for details."
        )

    return summary


# Define the DAG
with DAG(
    dag_config['dag_id'],
    default_args=default_args,
    description=dag_config['description'],
    schedule_interval=dag_config['schedule_interval'],
    catchup=dag_config['catchup'],
    tags=dag_config['tags'],
    max_active_runs=dag_config['max_active_runs'],
    max_active_tasks=dag_config['max_active_tasks'],
) as dag:
    process_task = PythonOperator(
        task_id='process_replenishment',
        python_callable=process_replenishment,
        provide_context=True,
    )
