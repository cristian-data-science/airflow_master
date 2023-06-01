# Utiliza la imagen original de Airflow
FROM apache/airflow:2.3.0

# Instala las librerías adicionales
RUN pip install pandas python-dotenv pyodbc
