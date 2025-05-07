# Utiliza la imagen original de Airflow
FROM apache/airflow:2.7.3

RUN python -m pip install --upgrade pip

# Instala las librerías adicionales
RUN pip install pandas python-dotenv pyodbc
RUN pip install 'pyarrow>=10.0.1,<10.1.0'
RUN pip install snowflake-connector-python
RUN pip install pymssql==2.3.1
RUN pip install pytz
