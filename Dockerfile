# Utiliza la imagen original de Airflow
FROM apache/airflow:2.7.3

# Instala las librerías adicionales
RUN pip install pandas python-dotenv pyodbc
RUN pip install snowflake-connector-python
