# Airflow Patagonia Master

Este proyecto implementa un robusto pipeline de datos y procesos ETL (Extracción, Transformación, Carga) utilizando Apache Airflow, Docker y diversas herramientas de integración de datos. Está diseñado para manejar datos de múltiples fuentes, incluyendo Shopify, D365 y OMS .

## 🚀 Características

- Entorno Airflow containerizado usando Docker
- Integración con fuentes de datos de Shopify, ERP y OMS
- Flujos de trabajo automatizados para procesamiento y carga de datos
- Optimizado para manejar datos de retail, datos de clientes e información de pedidos
- Arquitectura escalable con DAGs separados para diferentes procesos de datos

## 🛠️ Stack Tecnológico

- Apache Airflow 2.7.3
- Python 3.x
- PostgreSQL 13
- Docker y Docker Compose
- Conector de Snowflake
- pandas, pyarrow y otras bibliotecas de procesamiento de datos

## 📂 Estructura del Proyecto

```
.
├── .github/workflows/    # Flujos de trabajo de GitHub Actions
├── dags/                 # Definiciones de DAGs de Airflow
│   ├── config/           # Archivos de configuración
│   └── utils/            # Funciones de utilidad
├── Dockerfile            # Definición de imagen personalizada de Airflow
├── docker-compose.yaml   # Configuración de Docker Compose
├── .env                  # Variables de entorno (no rastreadas en git)
├── .gitignore            # Reglas de ignorado de Git
└── README.md             # Este archivo
```

## 🔧 Configuración e Instalación

1. Asegúrate de tener Docker y Docker Compose instalados en tu sistema.
2. Clona este repositorio:
   ```
   git clone <url-del-repositorio>
   ```
3. Crea un archivo `.env` en el directorio raíz con las variables de entorno necesarias.
4. Construye e inicia los servicios:
   ```
   docker-compose up -d --build
   ```

## 📊 Flujos de Datos

- **Datos de Shopify**: 
  - Procesamiento de datos de clientes (`shopify_customer_data.py`)
  - Extracción de datos de pedidos (`shopify_order_data.py`)
  - Gestión de variantes de productos (`shopify_product_variants.py`)

- **Datos de ERP**:
  - Sincronización de datos de clientes (`erp_customer_data.py`)
  - Procesamiento de datos de líneas de venta (`erp_salesline_data.py`, `erp_processed_salesline_data.py`)

- **Datos de OMS**:
  - Gestión de datos de pedidos (`oms_order_data.py`)
  - Seguimiento de datos de incidencias (`oms_incidence_data.py`)

- **Integración con Klaviyo**:
  - Carga de datos de ventas minoristas (`klaviyo_retail_sales_loading.py`)

## 🔄 CI/CD

El proyecto utiliza GitHub Actions para integración y despliegue continuos. Los flujos de trabajo se definen en el directorio `.github/workflows`.

## 🐳 Configuración de Docker

- Imagen personalizada de Airflow con dependencias adicionales
- Base de datos PostgreSQL para metadatos de Airflow
- Servicios separados para el servidor web de Airflow, el planificador y la inicialización

---

Para obtener información más detallada sobre cada componente, consulta los archivos individuales y sus comentarios.
